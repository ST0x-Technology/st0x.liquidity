//! Single-chain CCTP operations.

use alloy::primitives::{Address, B256, Bytes, FixedBytes, TxHash, U256};
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, TransactionReceipt};
use alloy::sol;
use alloy::sol_types::SolEvent;
use std::time::{Duration, Instant};
use tokio::time::{MissedTickBehavior, interval};
use tracing::{debug, info, trace, warn};

#[cfg(test)]
use st0x_evm::Evm;
use st0x_evm::{
    EvmError, IntoErrorRegistry, NODE_SYNC_MAX_ATTEMPTS, NODE_SYNC_POLL_INTERVAL, Wallet,
    wait_for_node_sync,
};

use super::{
    CctpError, CctpReceivedMessage, FAST_TRANSFER_THRESHOLD, MessageTransmitterV2, MintReceipt,
    TokenMessengerV2, parse_received_message,
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

/// Number of [`CCTP_RECOVERY_LOG_BLOCK_CHUNK`]-sized chunks
/// [`CctpEndpoint::reconstruct_existing_mint`]'s retry scans backward from
/// the current head before giving up, bounding a single scan attempt's cost.
///
/// This floor is safe specifically because `reconstruct_existing_mint` only
/// runs after [`CctpEndpoint::recover_already_minted`]'s probe loop has
/// itself just observed `usedNonces()` flip to consumed, within the current
/// recovery window (production: ~2 minutes). The matching `MessageReceived`
/// log is therefore necessarily within the last few chunks of the current
/// head, not somewhere deep in chain history, so three chunks (60,000
/// blocks -- many hours even on a fast chain like Base) is a wide margin
/// over the sub-minute recency this bound relies on, while still cutting a
/// worst-case scan from thousands of chunks to three.
///
/// [`CctpEndpoint::find_existing_mint`]'s own scan is NOT bounded by this:
/// it is a proactive check run during crash-recovery resume, which may be
/// re-checking a transfer that stalled for an arbitrary, unbounded amount of
/// time (e.g. days, waiting on an operator) before this code ever runs, so
/// the "the mint is recent" argument above does not hold there.
const RECONSTRUCTION_SCAN_LOOKBACK_CHUNKS: u64 = 3;

/// Delay between the `usedNonces()` probes that
/// [`CctpEvm::recover_already_minted`] runs after a failed `receiveMessage`.
///
/// This is the production default (see [`MintRecoveryConfig::defaults`]);
/// tests override the cadence via
/// [`CctpEndpoint::with_mint_recovery_config`] to pin behaviour against a
/// short, deterministic interval instead of racing or pausing real time
/// against this multi-minute production value.
pub(super) const MINT_RECOVERY_PROBE_INTERVAL: Duration = Duration::from_secs(10);

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
const MINT_RECOVERY_PROBES: u32 = 13;

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
    /// `probe_interval`.
    pub(super) probes: u32,
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

    /// Zero-grace, single-miss, fast-poll variant for tests: an absent tx
    /// concludes `Dropped` immediately rather than waiting out the production
    /// 30 s grace window. Shared by the bridge's own tests and downstream
    /// consumers' resume tests (via [`CctpBridge::with_fast_burn_drop_policy`]).
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
            node_sync_poll_interval: NODE_SYNC_POLL_INTERVAL,
            burn_drop_config: BurnDropConfig::defaults(),
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
        let receipt = self.wallet.confirm::<Registry>(tx_hash).await?;

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

    /// Resolves the on-chain status of a broadcast burn tx for crash-safe resume,
    /// using this endpoint's configured drop policy (`burn_drop_config`).
    pub(super) async fn burn_status(
        &self,
        tx_hash: TxHash,
    ) -> Result<crate::BurnTxStatus, CctpError> {
        self.burn_status_with_config(tx_hash, self.burn_drop_config)
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
    /// - no receipt and tx absent from the mempool -> KEEPS POLLING (does NOT
    ///   return `Pending` early); only once `config.grace` has elapsed AND
    ///   `config.consecutive_misses` consecutive post-grace absences are observed
    ///   does it return [`BurnTxStatus::Dropped`].
    ///
    /// A still-pending burn (broadcast but unmined) thus never reports `Dropped`,
    /// so the caller never re-burns a tx that may still land -- the exact
    /// double-burn hazard a chain-head-margin heuristic alone cannot rule out.
    pub(super) async fn burn_status_with_config(
        &self,
        tx_hash: TxHash,
        config: BurnDropConfig,
    ) -> Result<crate::BurnTxStatus, CctpError> {
        let provider = self.wallet.provider();
        let start = Instant::now();
        let mut consecutive_misses = 0u32;
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
                let confirmed = self.wallet.await_receipt(tx_hash).await?;
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

            // Absent from both receipt and mempool. Keep polling within this call
            // rather than returning early: only after the grace period AND
            // `consecutive_misses` consecutive absences is the absence trusted as a
            // true drop. Before that, a transient absence (lagging node, not-yet-
            // propagated tx) keeps polling so a tx that mines late is still seen and
            // a still-pending tx is never re-burned.
            if start.elapsed() >= config.grace {
                consecutive_misses += 1;
                if consecutive_misses >= config.consecutive_misses {
                    return Ok(crate::BurnTxStatus::Dropped);
                }
            }
        }
    }

    /// Scans for a `DepositForBurn` event from this endpoint's wallet at or
    /// after `from_block`, returning the transaction hash of the most recent
    /// match.
    ///
    /// Crash-safe burn recovery: a transfer records the chain head before the
    /// burn, so on resume this detects an already-submitted burn instead of
    /// re-burning (which would burn USDC twice with at most one mint). Matches on
    /// `(depositor, amount, destinationDomain, mintRecipient)` so an adopted burn
    /// is provably this transfer's -- not merely a same-amount burn from the same
    /// wallet to a different destination. The head is captured before the burn, so
    /// this transfer's burn lands strictly after `from_block`; the scan excludes the
    /// `from_block` block itself so an earlier identical burn is never adopted --
    /// consistent with [`find_recent_mint`](Self::find_recent_mint).
    ///
    /// Returns `Ok(None)` ONLY when the queried node is confirmations-deep past
    /// `from_block` and repeated scans agree the burn is absent; a node that may
    /// be lagging (the dRPC load-balancing hazard) yields a retryable
    /// [`CctpError::ScanInconclusive`] instead, so the caller never re-burns off a
    /// single stale empty `eth_getLogs`.
    pub(super) async fn find_recent_burn(
        &self,
        amount: U256,
        dest_domain: u32,
        recipient: Address,
        from_block: u64,
    ) -> Result<Option<TxHash>, CctpError> {
        let depositor = self.wallet.address();
        let mint_recipient = FixedBytes::<32>::left_padding_from(recipient.as_slice());
        let filter = Filter::new()
            .from_block(from_block)
            .address(self.token_messenger_address)
            .event_signature(TokenMessengerV2::DepositForBurn::SIGNATURE_HASH);

        for attempt in 1..=SCAN_ATTEMPTS {
            let logs = self.wallet.provider().get_logs(&filter).await?;

            for log in logs.iter().rev() {
                let decoded = log.log_decode::<TokenMessengerV2::DepositForBurn>()?;
                let event = decoded.data();

                if event.depositor == depositor
                    && event.amount == amount
                    && event.destinationDomain == dest_domain
                    && event.mintRecipient == mint_recipient
                    && log.block_number.is_some_and(|block| block > from_block)
                    && let Some(tx_hash) = log.transaction_hash
                {
                    debug!(target: "bridge", %tx_hash, from_block, "Found existing burn during resume");
                    return Ok(Some(tx_hash));
                }
            }

            // A single empty eth_getLogs from a load-balanced node is not
            // authoritative (dRPC lag). Only conclude a true absence once the head
            // is confirmations-deep past from_block AND repeated scans agree; else
            // retry, and if still inconclusive return a retryable error so the
            // caller never re-burns off a stale empty result.
            let head = self.wallet.provider().get_block_number().await?;
            let caught_up = head >= from_block.saturating_add(SCAN_FINALITY_MARGIN);

            if caught_up && attempt == SCAN_ATTEMPTS {
                return Ok(None);
            }

            if attempt < SCAN_ATTEMPTS {
                tokio::time::sleep(SCAN_RETRY_BACKOFF).await;
            }
        }

        Err(CctpError::ScanInconclusive { from_block })
    }

    /// Scans for a `MintAndWithdraw` event minting to `recipient` strictly after
    /// `from_block`, returning the receipt of the most recent match.
    ///
    /// Used for crash-safe mint recovery: a transfer records the destination
    /// chain head before submitting the mint, so on resume this detects an
    /// already-submitted mint instead of re-minting (which reverts on the
    /// already-used CCTP nonce). The mint is recorded strictly after the captured
    /// head, so the scan excludes the head block itself -- this bounds the window
    /// to blocks mined after capture and prevents adopting an earlier mint to the
    /// same wallet. Matching on `mintRecipient` and `mintToken` plus this bound
    /// and the single-in-flight invariant (one USDC rebalance at a time) guarantees the
    /// match is the resuming transfer's mint. The event carries the actual amount
    /// received and fee collected, so the adopted mint records the same inventory
    /// figures a fresh mint would.
    pub(super) async fn find_recent_mint(
        &self,
        recipient: Address,
        from_block: u64,
    ) -> Result<Option<MintReceipt>, CctpError> {
        let filter = Filter::new()
            .from_block(from_block)
            .address(self.token_messenger_address)
            .event_signature(TokenMessengerV2::MintAndWithdraw::SIGNATURE_HASH);

        let logs = self.wallet.provider().get_logs(&filter).await?;

        for log in logs.iter().rev() {
            let decoded = log.log_decode::<TokenMessengerV2::MintAndWithdraw>()?;
            let event = decoded.data();

            if event.mintRecipient == recipient
                && event.mintToken == self.usdc_address
                && log.block_number.is_some_and(|block| block > from_block)
                && let Some(tx_hash) = log.transaction_hash
            {
                debug!(target: "bridge", %tx_hash, from_block, "Found existing mint during resume");
                return Ok(Some(MintReceipt {
                    tx: tx_hash,
                    amount: event.amount,
                    fee_collected: event.feeCollected,
                }));
            }
        }

        Ok(None)
    }

    /// Returns the current head of this endpoint's chain.
    pub(super) async fn current_block(&self) -> Result<u64, CctpError> {
        Ok(self.wallet.provider().get_block_number().await?)
    }

    /// Returns the block in which `tx_hash` was mined on this endpoint's chain.
    ///
    /// Used to derive the lower bound for [`find_recent_usdc_transfer`] from the
    /// known mint tx: the deposit send to Alpaca lands at or after the mint's
    /// block, so the mint block bounds the transfer scan exactly the way the
    /// captured head bounds [`find_recent_burn`]. Confirmation-aware: it polls via
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

    /// Sends `amount` of this endpoint's USDC from the wallet to `to`, waiting
    /// for the configured confirmation depth, and returns the transfer tx hash.
    ///
    /// This is the fund-moving leg of a BaseToAlpaca deposit: the CCTP mint
    /// credits the bot wallet, and this transfer forwards the minted USDC to
    /// Alpaca's deposit address. Reuses [`Wallet::submit`] so nonce handling and
    /// confirmation depth match every other write path; a revert is decoded via
    /// `Registry`.
    pub(super) async fn send_usdc<Registry: IntoErrorRegistry>(
        &self,
        to: Address,
        amount: U256,
    ) -> Result<TxHash, CctpError> {
        let receipt = self
            .wallet
            .submit::<Registry, _>(
                self.usdc_address,
                IERC20::transferCall { to, amount },
                "USDC deposit to Alpaca",
            )
            .await?;

        Ok(receipt.transaction_hash)
    }

    /// Scans for a USDC `Transfer(from, to, value == amount)` at or after
    /// `from_block`, returning the most recent matching transaction hash.
    ///
    /// Crash-safe deposit-send recovery: the BaseToAlpaca deposit leg records the
    /// mint block before sending USDC to Alpaca, so on resume this detects an
    /// already-submitted send instead of re-sending (which would forward the
    /// minted USDC twice). The deposit send lands at or after the mint, so the
    /// match is bounded to `from_block` (the mint's block) onward. Matching on the
    /// indexed `(from, to)` topics plus the exact `value` -- combined with the
    /// single-USDC-rebalance-in-flight invariant -- guarantees an adopted transfer
    /// is this deposit's, not an unrelated same-amount transfer.
    ///
    /// Returns `Ok(None)` ONLY when the queried node is confirmations-deep past
    /// `from_block` and repeated scans agree the transfer is absent; a node that
    /// may be lagging (the dRPC load-balancing hazard) yields a retryable
    /// [`CctpError::ScanInconclusive`], so the caller never re-sends off a single
    /// stale empty `eth_getLogs`.
    pub(super) async fn find_recent_usdc_transfer(
        &self,
        from: Address,
        to: Address,
        amount: U256,
        from_block: u64,
    ) -> Result<Option<TxHash>, CctpError> {
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

            for log in logs.iter().rev() {
                let decoded = log.log_decode::<IERC20::Transfer>()?;
                let event = decoded.data();

                if event.value == amount
                    && log.block_number.is_some_and(|block| block >= from_block)
                    && let Some(tx_hash) = log.transaction_hash
                {
                    debug!(target: "bridge", %tx_hash, from_block, "Found existing USDC deposit transfer during resume");
                    return Ok(Some(tx_hash));
                }
            }

            // A single empty eth_getLogs from a load-balanced node is not
            // authoritative (dRPC lag). Only conclude a true absence once the head
            // is confirmations-deep past from_block AND repeated scans agree; else
            // retry, and if still inconclusive return a retryable error so the
            // caller never re-sends off a stale empty result.
            let head = self.wallet.provider().get_block_number().await?;
            let caught_up = head >= from_block.saturating_add(SCAN_FINALITY_MARGIN);

            if caught_up && attempt == SCAN_ATTEMPTS {
                return Ok(None);
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
    pub(super) async fn find_existing_mint<Registry: IntoErrorRegistry>(
        &self,
        direction: BridgeDirection,
        message: &[u8],
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

        // No lower bound: a crash-recovery resume may be re-checking a
        // transfer that stalled for an unbounded amount of time before this
        // runs, so the mint cannot be assumed recent here -- unlike
        // `reconstruct_existing_mint`'s bounded retry (see
        // `RECONSTRUCTION_SCAN_LOOKBACK_CHUNKS`'s doc).
        self.locate_mint_receipt(&received_message, None)
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
    async fn is_nonce_used<Registry: IntoErrorRegistry>(
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

    /// Locates and validates the mint receipt for a message whose nonce
    /// [`is_nonce_used`](Self::is_nonce_used) already confirmed consumed.
    /// Scans for the `MessageReceived` log matching the attested source
    /// domain and body, then validates the mint tx did not revert and emitted
    /// `MintAndWithdraw`.
    ///
    /// `min_block` floors the backward scan; passed straight through to
    /// [`find_received_message_tx`](Self::find_received_message_tx), see its
    /// doc for when a floor is (and is not) safe to pass.
    async fn locate_mint_receipt(
        &self,
        received_message: &CctpReceivedMessage<'_>,
        min_block: Option<u64>,
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
    /// [`find_recent_burn`](Self::find_recent_burn) and
    /// [`find_recent_usdc_transfer`](Self::find_recent_usdc_transfer) already
    /// apply to their own `get_logs` scans. This runs at most once per
    /// `recover_already_minted` call (not once per probe). Each retry's scan
    /// is additionally floored at [`RECONSTRUCTION_SCAN_LOOKBACK_CHUNKS`]
    /// chunks behind the current head (falling back to an unbounded scan if
    /// the head read itself fails), so `SCAN_ATTEMPTS` retries are genuinely
    /// a handful of quick attempts instead of each one repeating a full
    /// backward walk to genesis.
    ///
    /// Any other failure means the nonce is authoritatively consumed but the
    /// receipt could not be validated (a reverted mint tx, a mismatched log, a
    /// missing event): this is returned as-is, and the caller reports it as
    /// [`CctpError::MintRecoveryInconclusive`] rather than a false "never
    /// minted" terminal failure, since the authoritative nonce read already
    /// proved the mint landed.
    async fn reconstruct_existing_mint(
        &self,
        received_message: &CctpReceivedMessage<'_>,
    ) -> Result<MintReceipt, CctpError> {
        let min_block = match self.current_block().await {
            Ok(head) => Some(head.saturating_sub(
                CCTP_RECOVERY_LOG_BLOCK_CHUNK.saturating_mul(RECONSTRUCTION_SCAN_LOOKBACK_CHUNKS),
            )),
            Err(error) => {
                warn!(
                    target: "bridge",
                    ?error,
                    "Failed to read the chain head to floor the reconstruction scan; \
                     falling back to an unbounded backward scan for this attempt"
                );
                None
            }
        };

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

        for probe in 1..=config.probes {
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
                        .reconstruct_existing_mint(&received_message)
                        .await
                        .map_err(|reconstruction_error| CctpError::MintRecoveryInconclusive {
                            probe_error: Box::new(reconstruction_error),
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
                probe_error: Box::new(last_probe_error.into()),
            });
        }

        let window = config
            .probe_interval
            .saturating_mul(config.probes.saturating_sub(1));
        let probes = config.probes;
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
    /// `min_block` floors how far back the scan walks: `None` (used by
    /// [`find_existing_mint`](Self::find_existing_mint)'s proactive
    /// crash-recovery check) leaves it unbounded, walking all the way to
    /// block 0 if needed, since that caller may be re-checking a transfer
    /// that stalled for an unbounded amount of time. `Some(block)` (used by
    /// [`reconstruct_existing_mint`](Self::reconstruct_existing_mint)'s
    /// retry, see [`RECONSTRUCTION_SCAN_LOOKBACK_CHUNKS`]) stops the walk
    /// there instead, since that caller only runs immediately after this
    /// same recovery attempt's own probe loop observed the nonce become
    /// consumed, so the matching log cannot be older than the floor. A hard
    /// limit is deliberately omitted in the unbounded case to avoid failing
    /// recovery when a deep reorg or lagging node pushes the log back.
    async fn find_received_message_tx(
        &self,
        source_domain: u32,
        nonce: B256,
        message_body: &[u8],
        min_block: Option<u64>,
    ) -> Result<(TxHash, u64), CctpError> {
        let floor = min_block.unwrap_or(0);
        let latest = self
            .wallet
            .provider()
            .get_block_number()
            .await
            .map_err(EvmError::from)?;
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
    /// lets resume tests classify an absent burn tx as `Dropped` immediately
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
    #[cfg(test)]
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
    use alloy::primitives::{Bloom, Log as PrimitiveLog};
    use alloy::rpc::types::Log;

    use super::*;

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
}

//! Single-chain CCTP operations.

use std::time::Duration;

use alloy::primitives::{Address, B256, Bytes, FixedBytes, TxHash, U256};
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, TransactionReceipt};
use alloy::sol;
use alloy::sol_types::SolEvent;
use tracing::{debug, info, trace, warn};

#[cfg(test)]
use st0x_evm::Evm;
use st0x_evm::{
    EvmError, IntoErrorRegistry, NODE_SYNC_MAX_ATTEMPTS, NODE_SYNC_POLL_INTERVAL, Wallet,
    wait_for_node_sync,
};

use super::{
    CctpError, FAST_TRANSFER_THRESHOLD, MessageTransmitterV2, MintReceipt, TokenMessengerV2,
    parse_received_message,
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

        let recipient_bytes32 = FixedBytes::<32>::left_padding_from(recipient.as_slice());

        // bytes32(0) allows any address to call receiveMessage() on destination.
        // See: https://github.com/circlefin/evm-cctp-contracts/blob/master/src/TokenMessenger.sol
        let destination_caller = FixedBytes::<32>::ZERO;

        let receipt = self
            .wallet
            .submit::<Registry, _>(
                self.token_messenger_address,
                TokenMessengerV2::depositForBurnCall {
                    amount,
                    destinationDomain: direction.dest_domain(),
                    mintRecipient: recipient_bytes32,
                    burnToken: self.usdc_address,
                    destinationCaller: destination_caller,
                    maxFee: max_fee,
                    minFinalityThreshold: FAST_TRANSFER_THRESHOLD,
                },
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

    /// Reconstructs the receipt of an already-executed mint for the attested
    /// `message`, or `None` if its nonce has not been consumed on this chain.
    ///
    /// Confirms via `usedNonces()` (structural ground truth, not the
    /// decoder/provider-dependent revert string) and matches the on-chain
    /// `MessageReceived` log against the attested message's source domain and
    /// body before trusting the recovered amounts. Used both proactively
    /// (crash-recovery resume, before minting) and reactively (after a
    /// `receiveMessage` revert, via `recover_already_minted`).
    pub(super) async fn find_existing_mint<Registry: IntoErrorRegistry>(
        &self,
        direction: BridgeDirection,
        message: &[u8],
    ) -> Result<Option<MintReceipt>, CctpError> {
        let received_message = parse_received_message(message)?;

        // CCTP V2 assigns the real nonce only at attestation; an unattested or
        // invalid message still carries the reserved zero nonce, which the
        // transmitter reports as used. Such a message was never minted.
        if received_message.nonce == B256::ZERO {
            return Ok(None);
        }

        // Authoritative gate: usedNonces() is non-zero once the nonce has been
        // consumed (the mint executed and emitted MintAndWithdraw below).
        let nonce_used = self
            .wallet
            .call::<Registry, _>(
                self.message_transmitter_address,
                MessageTransmitterV2::usedNoncesCall(received_message.nonce),
            )
            .await?;

        if nonce_used.is_zero() {
            return Ok(None);
        }

        if received_message.destination_domain != direction.dest_domain() {
            return Err(CctpError::MessageDestinationDomainMismatch {
                expected: direction.dest_domain(),
                actual: received_message.destination_domain,
            });
        }

        let (tx_hash, message_received_log_index) = self
            .find_received_message_tx(
                received_message.source_domain,
                received_message.nonce,
                received_message.message_body,
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

        Ok(Some(mint_receipt))
    }

    /// Reactive recovery for a `receiveMessage` revert: if the nonce was already
    /// minted, returns the existing receipt; otherwise (nonce not consumed, or
    /// the recovery probe could not conclusively reconstruct our mint) it
    /// re-surfaces the original `submit_error` rather than masking it with a
    /// probe error.
    pub(super) async fn recover_already_minted<Registry: IntoErrorRegistry>(
        &self,
        direction: BridgeDirection,
        message: &[u8],
        submit_error: EvmError,
    ) -> Result<MintReceipt, CctpError> {
        match self
            .find_existing_mint::<Registry>(direction, message)
            .await
        {
            Ok(Some(mint_receipt)) => Ok(mint_receipt),
            Ok(None) => Err(submit_error.into()),
            Err(probe_error) => {
                warn!(
                    target: "bridge",
                    ?probe_error,
                    "CCTP mint recovery probe failed; surfacing original submit error"
                );
                Err(submit_error.into())
            }
        }
    }

    /// Locates the `receiveMessage` transaction that minted `nonce` and returns
    /// its hash alongside the matched `MessageReceived` log index (used to
    /// correlate the right `MintAndWithdraw` within a multicall transaction).
    ///
    /// The backward scan has no upper bound and can in principle reach block 0,
    /// but this is only invoked during crash recovery immediately after a
    /// submit failure, so the matching mint is always recent and found in the
    /// first few chunks. A hard scan limit is deliberately omitted to avoid
    /// failing recovery when a deep reorg or lagging node pushes the log back.
    async fn find_received_message_tx(
        &self,
        source_domain: u32,
        nonce: B256,
        message_body: &[u8],
    ) -> Result<(TxHash, u64), CctpError> {
        let latest = self
            .wallet
            .provider()
            .get_block_number()
            .await
            .map_err(EvmError::from)?;
        let mut to_block = latest;
        let mut saw_nonce = false;

        loop {
            let from_block =
                to_block.saturating_sub(CCTP_RECOVERY_LOG_BLOCK_CHUNK.saturating_sub(1));
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

            if from_block == 0 {
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

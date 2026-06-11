//! Shared transaction submission with nonce-race serialization and
//! replacement-fee escalation.
//!
//! Both wallet backends (`turnkey`, `local-signer`) duplicate the same
//! send path, so the recovery logic lives here and both call
//! [`send_with_recovery`]. It hardens submission against two failure
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
//! The pre-existing "nonce too low" handling (external nonce advance,
//! e.g. a CLI submitting from the same wallet) is preserved.

use alloy::eips::eip1559::Eip1559Estimation;
use alloy::primitives::{Address, Bytes, TxHash};
use alloy::providers::Provider;
use alloy::rpc::types::TransactionRequest;
use async_trait::async_trait;
use futures::lock::Mutex;
use tracing::{error, info, warn};

use crate::EvmError;
use crate::nonce::ResettableNonceManager;

/// Number of escalating-fee resubmit attempts after the first
/// "replacement transaction underpriced" rejection. Each attempt bumps
/// the fee further; exhausting them is a hard error.
const MAX_REPLACEMENT_RESUBMITS: u32 = 4;

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

    /// The lowest unconfirmed nonce (the `latest`-block transaction
    /// count) -- the nonce a stuck pending transaction occupies.
    async fn latest_nonce(&self, address: Address) -> Result<u64, EvmError>;

    /// Current EIP-1559 fee estimate from the network.
    async fn estimate_fees(&self) -> Result<Eip1559Estimation, EvmError>;
}

#[async_trait]
impl<P: Provider> TxSubmitter for P {
    async fn submit(&self, tx: TransactionRequest) -> Result<TxHash, EvmError> {
        let pending = self.send_transaction(tx).await?;
        Ok(*pending.tx_hash())
    }

    async fn latest_nonce(&self, address: Address) -> Result<u64, EvmError> {
        Ok(self.get_transaction_count(address).await?)
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
/// signing provider, `nonce_manager` its [`ResettableNonceManager`], and
/// `send_lock` serializes all sends from this wallet.
pub(crate) async fn send_with_recovery<Submitter>(
    submitter: &Submitter,
    nonce_manager: &ResettableNonceManager,
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

    let tx = TransactionRequest::default()
        .to(contract)
        .input(calldata.clone().into());

    let error = match submitter.submit(tx).await {
        Ok(tx_hash) => {
            info!(target: "wallet", %tx_hash, note, "Transaction submitted");
            return Ok(tx_hash);
        }
        Err(error) => error,
    };

    if error.is_nonce_too_low() {
        warn!(target: "wallet", %contract, note, "Nonce too low — \
            invalidating cache and retrying (external nonce change detected)");
        nonce_manager.invalidate();

        let retry_tx = TransactionRequest::default()
            .to(contract)
            .input(calldata.into());
        let tx_hash = submitter.submit(retry_tx).await.inspect_err(|error| {
            error!(target: "wallet", %error, note, "Nonce-too-low retry also failed \
                -- invalidating nonce cache");
            nonce_manager.invalidate();
        })?;
        info!(target: "wallet", %tx_hash, note, "Transaction submitted");
        return Ok(tx_hash);
    }

    if error.is_replacement_underpriced() {
        warn!(target: "wallet", %contract, note, %error, "Transaction underpriced \
            -- a pending transaction occupies the nonce; resubmitting with bumped fee");
        return resubmit_with_bumped_fee(
            submitter,
            nonce_manager,
            address,
            contract,
            calldata,
            note,
        )
        .await;
    }

    warn!(target: "wallet", %contract, note, %error, "Transaction rejected by RPC \
        -- invalidating nonce cache to prevent nonce gap");
    nonce_manager.invalidate();
    Err(error)
}

/// Resubmit a transaction at the stuck nonce with an escalating fee until
/// the node accepts the replacement or the attempt budget is exhausted.
///
/// Each attempt re-estimates the network fee, bumps it, and pins the
/// stuck nonce explicitly so the filler targets that transaction for
/// replacement.
async fn resubmit_with_bumped_fee<Submitter>(
    submitter: &Submitter,
    nonce_manager: &ResettableNonceManager,
    address: Address,
    contract: Address,
    calldata: Bytes,
    note: &str,
) -> Result<TxHash, EvmError>
where
    Submitter: TxSubmitter,
{
    // Invalidate on a setup failure (nonce fetch / fee estimate) so a
    // caller retry re-targets the stuck nonce to replace it, rather than
    // letting the cached nonce advance and queue the retry behind it.
    let nonce = submitter
        .latest_nonce(address)
        .await
        .inspect_err(|_| nonce_manager.invalidate())?;

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
                return Ok(tx_hash);
            }
            Err(error) => error,
        };

        if error.is_replacement_underpriced() {
            warn!(target: "wallet", %contract, note, attempt, "Replacement still \
                underpriced; escalating fee");
            continue;
        }

        // "nonce too low" here means the stuck transaction confirmed
        // while we were escalating, so the pinned nonce is now stale. Any
        // other error is non-recoverable. Either way, invalidate the
        // cache and surface it.
        nonce_manager.invalidate();
        return Err(error);
    }

    nonce_manager.invalidate();
    error!(target: "wallet", %contract, note, attempts = MAX_REPLACEMENT_RESUBMITS,
        "Replacement transaction still underpriced after exhausting fee-bump budget");
    Err(EvmError::ReplacementUnderpriced {
        attempts: MAX_REPLACEMENT_RESUBMITS,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use alloy::primitives::{Bytes, TxHash, address};
    use alloy::rpc::json_rpc::ErrorPayload;
    use alloy::transports::RpcError;

    use super::*;

    const CONTRACT: Address = address!("00000000000000000000000000000000000000c1");
    const WALLET: Address = address!("00000000000000000000000000000000000000a9");
    const STUCK_NONCE: u64 = 7;

    fn rpc_error(message: &'static str) -> EvmError {
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

    fn reverted() -> EvmError {
        rpc_error("execution reverted")
    }

    fn base_fees() -> Eip1559Estimation {
        Eip1559Estimation {
            max_fee_per_gas: 1_000_000_000,
            max_priority_fee_per_gas: 100_000_000,
        }
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
        fail_latest_nonce: bool,
        fail_estimate_fees: bool,
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
                fail_latest_nonce: false,
                fail_estimate_fees: false,
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
            self.fail_latest_nonce = true;
            self
        }

        fn with_failing_estimate_fees(mut self) -> Self {
            self.fail_estimate_fees = true;
            self
        }

        fn sent(&self) -> Vec<TransactionRequest> {
            self.sent.lock().expect("sent lock").clone()
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

        async fn latest_nonce(&self, _address: Address) -> Result<u64, EvmError> {
            if self.fail_latest_nonce {
                return Err(rpc_error("connection reset"));
            }
            Ok(STUCK_NONCE)
        }

        async fn estimate_fees(&self) -> Result<Eip1559Estimation, EvmError> {
            if self.fail_estimate_fees {
                return Err(rpc_error("connection reset"));
            }
            Ok(self.fees)
        }
    }

    async fn run(mock: &MockSubmitter) -> Result<TxHash, EvmError> {
        let nonce_manager = ResettableNonceManager::default();
        let send_lock = Mutex::new(());
        send_with_recovery(
            mock,
            &nonce_manager,
            &send_lock,
            WALLET,
            CONTRACT,
            Bytes::from_static(b"calldata"),
            "test",
        )
        .await
    }

    #[tokio::test]
    async fn first_send_success_returns_hash_without_resubmit() {
        let hash = TxHash::repeat_byte(0x11);
        let mock = MockSubmitter::new(vec![Ok(hash)]);

        let result = run(&mock).await.unwrap();

        assert_eq!(result, hash);
        let sent = mock.sent();
        assert_eq!(sent.len(), 1, "only one submit expected");
        assert_eq!(sent[0].nonce, None, "base send must not pin a nonce");
        assert_eq!(
            sent[0].max_fee_per_gas, None,
            "base send must not set a fee"
        );
    }

    #[tokio::test]
    async fn underpriced_then_success_resubmits_at_stuck_nonce_with_bumped_fee() {
        let hash = TxHash::repeat_byte(0x22);
        let mock = MockSubmitter::new(vec![Err(underpriced()), Ok(hash)]);

        let result = run(&mock).await.unwrap();

        assert_eq!(result, hash);
        let sent = mock.sent();
        assert_eq!(sent.len(), 2, "base send + one resubmit");

        let resubmit = &sent[1];
        assert_eq!(
            resubmit.nonce,
            Some(STUCK_NONCE),
            "resubmit must pin the stuck nonce to replace it"
        );
        // First bump is +15% over the network estimate.
        assert_eq!(resubmit.max_fee_per_gas, Some(1_000_000_000 * 115 / 100));
        assert_eq!(
            resubmit.max_priority_fee_per_gas,
            Some(100_000_000 * 115 / 100)
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

        // Resubmits escalate 15%, 30%, 45%, 60% over the estimate.
        let expected_bumps = [115, 130, 145, 160];
        for (index, pct) in expected_bumps.iter().enumerate() {
            let resubmit = &sent[index + 1];
            assert_eq!(resubmit.nonce, Some(STUCK_NONCE));
            assert_eq!(
                resubmit.max_fee_per_gas,
                Some(1_000_000_000 * pct / 100),
                "attempt {} fee bump",
                index + 1
            );
        }
    }

    #[tokio::test]
    async fn nonce_too_low_invalidates_and_retries_once() {
        let hash = TxHash::repeat_byte(0x33);
        let mock = MockSubmitter::new(vec![Err(nonce_too_low()), Ok(hash)]);

        let result = run(&mock).await.unwrap();

        assert_eq!(result, hash);
        let sent = mock.sent();
        assert_eq!(sent.len(), 2, "base send + one nonce-too-low retry");
        assert_eq!(sent[1].nonce, None, "nonce-too-low retry refills the nonce");
    }

    #[tokio::test]
    async fn nonce_too_low_retry_failure_returns_error() {
        let mock = MockSubmitter::new(vec![Err(nonce_too_low()), Err(reverted())]);

        let error = run(&mock).await.unwrap_err();

        assert!(
            matches!(&error, EvmError::Transport(rpc) if rpc.as_error_resp().is_some_and(|payload| payload.message.contains("execution reverted"))),
            "expected the retry's error, got {error:?}"
        );
        assert_eq!(mock.sent().len(), 2);
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
    async fn latest_nonce_failure_during_resubmit_surfaces_error() {
        // If fetching the stuck nonce fails, the resubmit loop must abort
        // and surface the RPC error rather than attempting a replacement.
        let mock = MockSubmitter::new(vec![Err(underpriced())]).with_failing_latest_nonce();

        let error = run(&mock).await.unwrap_err();

        assert!(
            matches!(&error, EvmError::Transport(rpc) if rpc.as_error_resp().is_some_and(|payload| payload.message.contains("connection reset"))),
            "expected the nonce-fetch RPC error, got {error:?}"
        );
        assert_eq!(
            mock.sent().len(),
            1,
            "no replacement should be sent when the nonce fetch fails"
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
        let send_lock = Mutex::new(());

        let first = send_with_recovery(
            &mock,
            &nonce_manager,
            &send_lock,
            WALLET,
            CONTRACT,
            Bytes::from_static(b"a"),
            "a",
        );
        let second = send_with_recovery(
            &mock,
            &nonce_manager,
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
}

//! Equity-specific trigger types and logic.

use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::warn;

use st0x_event_sorcery::{Projection, ProjectionError, SendError};
use st0x_execution::Symbol;
use st0x_wrapper::WrapperError;

use super::allocation::EquityPlanError;
use super::{RebalancingService, TokenAddressError};
use crate::conductor::job::{Job, JobQueue, Label, QueuePushError};
use crate::inventory::EquityVenuesError;
use crate::position::{Position, PriceObservation};

/// Why an equity trigger failed.
#[derive(Debug, thiserror::Error)]
pub(crate) enum EquityTriggerError {
    #[error("no wrapper is wired for {chain}, so its equity ratios cannot be read")]
    UnwiredWrapper { chain: st0x_evm::Chain },
    #[error(transparent)]
    Venues(#[from] EquityVenuesError),
    #[error(transparent)]
    TokenAddress(#[from] TokenAddressError),
    #[error(transparent)]
    Wrapper(#[from] WrapperError),
    #[error(transparent)]
    Plan(#[from] EquityPlanError),
    #[error("position authority is not wired")]
    PositionAuthorityNotWired,
    #[error(transparent)]
    PositionReservation(#[from] SendError<Position>),
    #[error("failed to read the symbol's last price: {0}")]
    LastPrice(#[from] ProjectionError<Position>),
}

/// Reads a symbol's last onchain fill price, block-timestamped, so the
/// planner can value the minimum operation size.
#[async_trait]
pub(crate) trait LastPriceReader: Send + Sync {
    async fn last_price(
        &self,
        symbol: &Symbol,
    ) -> Result<Option<PriceObservation>, ProjectionError<Position>>;
}

#[async_trait]
impl LastPriceReader for Projection<Position> {
    async fn last_price(
        &self,
        symbol: &Symbol,
    ) -> Result<Option<PriceObservation>, ProjectionError<Position>> {
        Ok(self
            .load(symbol)
            .await?
            .and_then(|position| position.last_price))
    }
}

/// Test double: every symbol was last priced at the given price just now.
#[cfg(test)]
pub(crate) struct StubLastPrice(pub(crate) rain_math_float::Float);

#[cfg(test)]
#[async_trait]
impl LastPriceReader for StubLastPrice {
    async fn last_price(
        &self,
        _: &Symbol,
    ) -> Result<Option<PriceObservation>, ProjectionError<Position>> {
        let Self(price) = self;

        Ok(Some(PriceObservation {
            price: *price,
            observed_at: chrono::Utc::now(),
        }))
    }
}

/// Discriminates why the equity in-progress slot is held.
///
/// Allows recovery jobs to proceed when the slot is in `HeldForRecovery`
/// state, while still blocking new transfer triggers from starting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum GuardState {
    /// A transfer job (mint or redemption) is actively running.
    /// Blocks both new triggers and recovery jobs.
    ///
    /// The `generation` counter identifies which specific claim holds this
    /// slot. It is incremented each time a new claim is made, enabling
    /// `RecoveryGuard::Drop` to detect the ABA race: if a terminal event
    /// cleared the slot and a new transfer immediately claimed it, Drop sees
    /// a different `generation` and does not clobber the new claim.
    ActiveTransfer { generation: GuardGeneration },
    /// Tokens were received but post-receipt processing failed.
    /// A recovery job must run to wrap/deposit them.
    /// Blocks new triggers; does NOT block `UnwrappedEquityRecovery` or
    /// `WrappedEquityRecovery`.
    HeldForRecovery,
}

/// Identifies the exact process and claim that owns an active equity transfer.
///
/// The upper 32 bits are a random nonzero boot nonce and the lower 32 bits are
/// a monotonic per-process counter. Zero is reserved for persisted job payloads
/// created before generations were introduced; only a restored legacy owner
/// uses it. Startup reserves persisted counters before allocating new claims.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub(crate) struct GuardGeneration(u64);

impl GuardGeneration {
    pub(crate) const fn from_parts(boot_nonce: NonZeroU32, counter: u32) -> Self {
        Self((boot_nonce.get() as u64) << 32 | counter as u64)
    }

    pub(crate) const fn is_legacy(self) -> bool {
        self.0 == 0
    }

    pub(crate) const fn boot_nonce(self) -> u32 {
        let bytes = self.0.to_be_bytes();
        u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
    }

    pub(crate) const fn counter(self) -> u32 {
        let bytes = self.0.to_be_bytes();
        u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]])
    }
}

pub(crate) struct GenerationSource {
    boot_nonce: NonZeroU32,
    counter: AtomicU32,
}

impl GenerationSource {
    fn new() -> Self {
        let boot_nonce = loop {
            if let Some(boot_nonce) = NonZeroU32::new(rand::random()) {
                break boot_nonce;
            }
        };

        Self::with_counter(boot_nonce, 0)
    }

    const fn with_counter(boot_nonce: NonZeroU32, counter: u32) -> Self {
        Self {
            boot_nonce,
            counter: AtomicU32::new(counter),
        }
    }

    /// Excludes durable owners before startup creates any new claims.
    pub(crate) fn reserve(&self, generation: GuardGeneration) {
        let bytes = generation.0.to_be_bytes();
        let boot_nonce = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        if boot_nonce == self.boot_nonce.get() {
            let counter = u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
            self.counter.fetch_max(counter, Ordering::Relaxed);
        }
    }

    pub(super) fn next(&self) -> Option<GuardGeneration> {
        self.counter
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |counter| {
                counter.checked_add(1)
            })
            .ok()
            .map(|counter| GuardGeneration::from_parts(self.boot_nonce, counter + 1))
    }
}

pub(crate) static GUARD_GENERATION: LazyLock<GenerationSource> =
    LazyLock::new(GenerationSource::new);

/// Atomically removes an active transfer only when the exact owner still holds
/// the symbol. The reserved zero generation can remove only a restored legacy
/// zero-generation owner; it can never match a new process-generated claim.
pub(crate) fn remove_active_transfer(
    map: &RwLock<HashMap<Symbol, GuardState>>,
    symbol: &Symbol,
    expected_generation: GuardGeneration,
) -> bool {
    let mut guard = match map.write() {
        Ok(guard) => guard,
        Err(poisoned) => {
            warn!(
                %symbol,
                "equity_in_progress lock poisoned during exact-generation removal; \
                 recovering inner guard"
            );
            poisoned.into_inner()
        }
    };

    if guard.get(symbol)
        != Some(&GuardState::ActiveTransfer {
            generation: expected_generation,
        })
    {
        warn!(
            %symbol,
            ?expected_generation,
            current_owner = ?guard.get(symbol),
            "Equity transfer guard release skipped: ownership does not match"
        );
        return false;
    }

    guard.remove(symbol);
    true
}

/// RAII guard that holds an equity in-progress claim.
/// Automatically releases the claim on drop unless `defuse` is called.
pub(crate) struct InProgressGuard {
    /// The symbol this guard holds a claim for.
    symbol: Symbol,
    /// Shared reference to the in-progress map for cleanup on drop.
    in_progress: Arc<std::sync::RwLock<HashMap<Symbol, GuardState>>>,
    /// Generation token matching the `ActiveTransfer { generation }` this
    /// guard inserted. Used on Drop to avoid clobbering a newer claim.
    generation: GuardGeneration,
    /// When true, the guard will not release the claim on drop.
    defused: bool,
}

impl InProgressGuard {
    /// Attempts to claim the in-progress slot for a new transfer.
    ///
    /// Inserts `ActiveTransfer { generation }`. Returns `None` if any state
    /// is already present -- including `HeldForRecovery` -- so a new transfer
    /// cannot start while recovery is pending.
    pub(crate) fn try_claim_for_transfer(
        symbol: Symbol,
        in_progress: Arc<std::sync::RwLock<HashMap<Symbol, GuardState>>>,
    ) -> Option<Self> {
        let Some(generation) = GUARD_GENERATION.next() else {
            warn!(%symbol, "Equity guard generation counter exhausted; refusing transfer claim");
            return None;
        };
        {
            let mut guard = match in_progress.write() {
                Ok(guard) => guard,
                Err(poison) => poison.into_inner(),
            };

            if guard.contains_key(&symbol) {
                return None;
            }

            guard.insert(symbol.clone(), GuardState::ActiveTransfer { generation });
        }

        Some(Self {
            symbol,
            in_progress,
            generation,
            defused: false,
        })
    }

    /// Returns the generation token for this guard's `ActiveTransfer` slot.
    /// Used by the transfer job to store the generation in its payload so
    /// `mark_held_for_recovery` can detect if a newer transfer claimed the slot.
    pub(super) fn generation(&self) -> GuardGeneration {
        self.generation
    }

    /// Prevents the guard from releasing the claim on drop.
    /// Call this after successfully sending the operation.
    pub(super) fn defuse(mut self) {
        self.defused = true;
    }
}

impl Drop for InProgressGuard {
    fn drop(&mut self) {
        if !self.defused {
            remove_active_transfer(&self.in_progress, &self.symbol, self.generation);
        }
    }
}

/// Tracks how a [`RecoveryGuard`] was originally claimed, so Drop knows
/// what state to restore on failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecoveryClaimOrigin {
    /// Claimed from `HeldForRecovery` (deadlock-break path). On Drop, the
    /// slot is restored to `HeldForRecovery` so the next recovery attempt can
    /// claim it without a new transfer job starting in the meantime.
    HeldForRecovery,
    /// Claimed from absent (orphan path). On Drop, the slot is removed so
    /// the next inventory poll can start a fresh recovery or transfer job.
    Orphan,
}

/// RAII guard for recovery jobs. Holds the `equity_in_progress` slot for a
/// symbol.
///
/// **Normal drop (recovery failed or panicked):**
/// - Claimed from `HeldForRecovery`: restores to `HeldForRecovery` so
///   subsequent recovery retries can proceed but new transfer jobs cannot
///   start (preventing double-mints while tokens are still stranded).
/// - Claimed from absent (orphan): removes the entry entirely.
///
/// **After `release()` (recovery succeeded):** the entry is always removed
/// regardless of claim origin so the symbol is fully unblocked.
///
/// In all cases, Drop only acts if the slot still holds
/// `ActiveTransfer { generation: self.generation }` (i.e. this guard still
/// owns it). A concurrent terminal event that cleared the slot and a new
/// transfer claiming `ActiveTransfer` with a different generation are both
/// safely left untouched -- this closes the ABA race.
pub(crate) struct RecoveryGuard {
    symbol: Symbol,
    map: Arc<RwLock<HashMap<Symbol, GuardState>>>,
    /// Generation token matching the `ActiveTransfer` entry this guard
    /// inserted. Drop only acts when the map still holds this exact generation.
    generation: GuardGeneration,
    /// Tracks the state the slot was in before this guard claimed it.
    claim_origin: RecoveryClaimOrigin,
    /// When true, Drop removes the entry instead of restoring prior state.
    /// Set by `release()` after a successful recovery.
    released: bool,
}

impl RecoveryGuard {
    /// Signals successful completion. The slot is removed on drop regardless
    /// of `claim_origin`, fully unblocking the symbol for future transfers.
    ///
    /// Consuming `self` here triggers `Drop::drop` with `released == true`,
    /// which removes the map entry. No explicit drop or map write is needed.
    pub(crate) fn release(mut self) {
        self.released = true;
        // Drop fires here at end of scope, executing Drop::drop with released=true.
    }

    /// Returns `true` when this guard was claimed from a `HeldForRecovery` slot
    /// (the deadlock-break handoff) rather than from an absent slot (orphan).
    ///
    /// Recovery jobs use this to decide whether a no-balance skip must
    /// self-reschedule: the inventory reactor only re-dispatches a recovery on
    /// a POSITIVE wallet balance, so a `HeldForRecovery`-origin job that finds
    /// zero balance and simply drops would restore `HeldForRecovery` with no
    /// job left to drain it -- wedging the symbol. Re-enqueuing with a delay
    /// keeps polling until the balance resolves or the slot is cleared.
    pub(crate) fn claimed_from_held_for_recovery(&self) -> bool {
        self.claim_origin == RecoveryClaimOrigin::HeldForRecovery
    }
}

impl Drop for RecoveryGuard {
    fn drop(&mut self) {
        if self.released {
            remove_active_transfer(&self.map, &self.symbol, self.generation);
            return;
        }

        let mut guard = match self.map.write() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!(
                    symbol = %self.symbol,
                    "equity_in_progress lock poisoned during recovery guard drop; releasing \
                     entry from inner guard"
                );
                poisoned.into_inner()
            }
        };
        // Only act if this guard still owns the slot. We check the exact
        // generation token to close the ABA race: if (1) a terminal event
        // cleared the slot, (2) a new transfer claimed ActiveTransfer with a
        // different generation, and (3) this Drop fires, the generation
        // mismatch causes us to leave the new claim untouched.
        if guard.get(&self.symbol)
            != Some(&GuardState::ActiveTransfer {
                generation: self.generation,
            })
        {
            return;
        }

        match self.claim_origin {
            RecoveryClaimOrigin::HeldForRecovery => {
                // Recovery failed: restore to HeldForRecovery so retries can
                // claim the slot but a new transfer cannot start while tokens
                // are still stranded (prevents double-mints).
                guard.insert(self.symbol.clone(), GuardState::HeldForRecovery);
            }
            RecoveryClaimOrigin::Orphan => {
                // Orphan failure: no prior guard state; remove entirely so
                // inventory-driven recovery can restart on the next poll.
                guard.remove(&self.symbol);
            }
        }
    }
}

/// Two-path guard claim for recovery jobs.
///
/// Used by BOTH [`UnwrappedEquityRecovery`] and [`WrappedEquityRecovery`]: a
/// `PostReceipt` failure can strand tokens either UNWRAPPED (wrap reverted) or
/// WRAPPED (the wrap landed but its confirmation read failed under a stale
/// RPC), and the inventory location -- not the guard -- decides which recovery
/// job the reactor dispatches. Both must therefore be able to claim a
/// `HeldForRecovery` slot, and the atomic check-and-insert below makes
/// concurrent claims safe: the first transitions the slot to `ActiveTransfer`,
/// the second sees `ActiveTransfer` and reschedules.
///
/// 1. `HeldForRecovery` -> `ActiveTransfer`: the deadlock-break path -- a live
///    transfer job set `HeldForRecovery` and returned `Ok(())`; recovery owns
///    the slot now. On drop, the slot is restored to `HeldForRecovery` so
///    recovery retries can proceed but new transfer jobs cannot start.
/// 2. Absent -> `ActiveTransfer`: the orphan path -- no active transfer was in
///    progress; recovery detected a wallet balance outside a mint cycle. On
///    drop, the slot is removed entirely.
/// 3. `ActiveTransfer`: a live transfer job owns the slot; returns `None` so
///    the caller reschedules.
pub(crate) fn claim_guard_for_recovery_or_orphan(
    map: &Arc<RwLock<HashMap<Symbol, GuardState>>>,
    symbol: &Symbol,
) -> Option<RecoveryGuard> {
    let mut guard = match map.write() {
        Ok(guard) => guard,
        Err(poisoned) => {
            warn!(
                %symbol,
                "equity_in_progress lock poisoned in claim_guard_for_recovery_or_orphan; \
                 recovering inner guard so future recovery attempts can proceed"
            );
            poisoned.into_inner()
        }
    };

    let claim_origin = match guard.get(symbol) {
        Some(GuardState::ActiveTransfer { .. }) => {
            // A live transfer job owns the slot; reschedule.
            return None;
        }
        Some(GuardState::HeldForRecovery) => RecoveryClaimOrigin::HeldForRecovery,
        None => RecoveryClaimOrigin::Orphan,
    };

    let Some(generation) = GUARD_GENERATION.next() else {
        warn!(%symbol, "Equity guard generation counter exhausted; refusing recovery claim");
        return None;
    };
    guard.insert(symbol.clone(), GuardState::ActiveTransfer { generation });
    drop(guard);

    Some(RecoveryGuard {
        symbol: symbol.clone(),
        map: Arc::clone(map),
        generation,
        claim_origin,
        released: false,
    })
}

/// Per-symbol equity rebalancing check.
///
/// Carries the symbol to evaluate as the payload; every other
/// dependency the worker needs lives on [`RebalancingService`]. Per-symbol
/// failures retry/back off independently, and snapshots affecting multiple
/// symbols fan out into parallel work.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct EquityRebalancingCheck {
    pub symbol: Symbol,
}

pub(crate) type EquityRebalancingCheckJobQueue = JobQueue<EquityRebalancingCheck>;

#[derive(Debug, thiserror::Error)]
pub(crate) enum EquityRebalancingCheckJobError {
    #[error(transparent)]
    EquityTrigger(#[from] EquityTriggerError),
}

impl Job<RebalancingService> for EquityRebalancingCheck {
    type Output = ();
    type Error = EquityRebalancingCheckJobError;

    const WORKER_NAME: &'static str = "equity-rebalancing-check-worker";
    const PERFORM_TIMEOUT: Option<std::time::Duration> =
        Some(crate::conductor::job::DEFAULT_PERFORM_TIMEOUT);

    #[cfg(any(test, feature = "test-support"))]
    const JOB_KIND: crate::conductor::job::JobKind =
        crate::conductor::job::JobKind::EquityRebalancingCheck;

    fn label(&self) -> Label {
        Label::new(format!("EquityRebalancingCheck({})", self.symbol))
    }

    async fn perform(&self, trigger: &RebalancingService) -> Result<Self::Output, Self::Error> {
        trigger.check_and_trigger_equity(&self.symbol).await?;
        Ok(())
    }
}

/// Owns the equity-check queue and the domain-level operations
/// callers (the reactor, the conductor wiring) want: enqueue a check
/// for a symbol, cancel pending checks after a terminal event. Keeps
/// queue plumbing out of [`RebalancingService`] and out of the
/// conductor wiring.
#[derive(Clone)]
pub(crate) struct EquityRebalancingCheckScheduler {
    queue: EquityRebalancingCheckJobQueue,
}

impl EquityRebalancingCheckScheduler {
    pub(crate) fn new(pool: &apalis_sqlite::SqlitePool) -> Self {
        Self {
            queue: EquityRebalancingCheckJobQueue::new(pool),
        }
    }

    pub(crate) fn queue(&self) -> &EquityRebalancingCheckJobQueue {
        &self.queue
    }

    /// Best-effort enqueue. Failures are logged: an enqueue miss only
    /// delays the next imbalance check, which the next snapshot will
    /// re-trigger.
    pub(super) async fn enqueue_check(&self, symbol: Symbol) {
        let mut queue = self.queue.clone();
        if let Err(QueuePushError(error)) = queue.push(EquityRebalancingCheck { symbol }).await {
            warn!(target: "rebalance", %error, "Failed to enqueue EquityRebalancingCheck job");
        }
    }

    pub(super) async fn cancel_pending(&self) {
        self.queue.cancel_all_pending().await;
    }
}

/// Test helper: synchronously drain every pending equity-check row
/// the service enqueued, running each job's [`Job::perform`] and
/// marking the row `Done`.
#[cfg(test)]
pub(crate) async fn drain_pending_equity_jobs(
    service: &std::sync::Arc<RebalancingService>,
) -> Result<usize, EquityRebalancingCheckJobError> {
    let pool = service.equity_scheduler.queue().pool().clone();
    let mut processed = 0usize;

    let job_type = std::any::type_name::<EquityRebalancingCheck>();

    loop {
        let row: Option<(String, Vec<u8>)> = sqlx_apalis::query_as(
            "SELECT id, job FROM Jobs \
             WHERE status = 'Pending' AND job_type = ? \
             ORDER BY run_at LIMIT 1",
        )
        .bind(job_type)
        .fetch_optional(&pool)
        .await
        .expect("query pending equity-check jobs");

        let Some((id, payload)) = row else {
            break;
        };

        let job: EquityRebalancingCheck =
            serde_json::from_slice(&payload).expect("deserialize EquityRebalancingCheck payload");
        job.perform(service).await?;

        sqlx_apalis::query("UPDATE Jobs SET status = 'Done' WHERE id = ?")
            .bind(&id)
            .execute(&pool)
            .await
            .expect("mark equity-check job done");

        processed += 1;
    }

    Ok(processed)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use tracing_test::traced_test;

    use super::*;

    fn make_in_progress() -> Arc<std::sync::RwLock<HashMap<Symbol, GuardState>>> {
        Arc::new(std::sync::RwLock::new(HashMap::new()))
    }

    #[test]
    fn generation_source_combines_boot_nonce_and_monotonic_counter() {
        let source = GenerationSource::with_counter(NonZeroU32::new(7).unwrap(), 0);

        assert_eq!(
            source.next().unwrap(),
            GuardGeneration::from_parts(NonZeroU32::new(7).unwrap(), 1)
        );
        assert_eq!(
            source.next().unwrap(),
            GuardGeneration::from_parts(NonZeroU32::new(7).unwrap(), 2)
        );
    }

    #[test]
    fn generation_sources_from_different_boots_cannot_collide() {
        let first = GenerationSource::with_counter(NonZeroU32::new(7).unwrap(), 0);
        let second = GenerationSource::with_counter(NonZeroU32::new(8).unwrap(), 0);

        assert_ne!(first.next().unwrap(), second.next().unwrap());
    }

    #[test]
    fn generation_source_fails_at_counter_exhaustion() {
        let source = GenerationSource::with_counter(NonZeroU32::new(7).unwrap(), u32::MAX);

        assert_eq!(source.next(), None);
    }

    #[test]
    fn repeated_boot_nonce_skips_all_persisted_claims() {
        let boot_nonce = NonZeroU32::new(7).unwrap();
        let source = GenerationSource::with_counter(boot_nonce, 0);
        source.reserve(GuardGeneration::from_parts(boot_nonce, 41));
        source.reserve(GuardGeneration::from_parts(boot_nonce, 12));
        source.reserve(GuardGeneration::from_parts(
            NonZeroU32::new(8).unwrap(),
            u32::MAX,
        ));
        source.reserve(GuardGeneration::default());

        assert_eq!(
            source.next(),
            Some(GuardGeneration::from_parts(boot_nonce, 42))
        );

        let map = make_in_progress();
        let symbol = Symbol::new("AAPL").unwrap();
        let generation = source.next().unwrap();
        map.write()
            .unwrap()
            .insert(symbol.clone(), GuardState::ActiveTransfer { generation });
        assert!(!remove_active_transfer(
            &map,
            &symbol,
            GuardGeneration::from_parts(boot_nonce, 41)
        ));
        assert_eq!(
            map.read().unwrap().get(&symbol),
            Some(&GuardState::ActiveTransfer { generation })
        );

        source.reserve(GuardGeneration::from_parts(boot_nonce, u32::MAX));
        assert_eq!(source.next(), None);
    }

    #[test]
    #[traced_test]
    fn exact_generation_removal_rejects_other_owners() {
        let map = make_in_progress();
        let symbol = Symbol::new("AAPL").unwrap();
        let owner = GuardGeneration::from_parts(NonZeroU32::new(7).unwrap(), 1);
        let other_boot = GuardGeneration::from_parts(NonZeroU32::new(8).unwrap(), 1);
        map.write().unwrap().insert(
            symbol.clone(),
            GuardState::ActiveTransfer { generation: owner },
        );

        assert!(!remove_active_transfer(
            &map,
            &symbol,
            GuardGeneration::default()
        ));
        assert!(!remove_active_transfer(&map, &symbol, other_boot));
        assert!(logs_contain("Equity transfer guard release skipped"));
        assert!(logs_contain("symbol=AAPL"));
        assert!(logs_contain(
            "expected_generation=GuardGeneration(34359738369)"
        ));
        assert!(logs_contain("current_owner=Some(ActiveTransfer"));
        assert_eq!(
            map.read().unwrap().get(&symbol),
            Some(&GuardState::ActiveTransfer { generation: owner })
        );
        assert!(remove_active_transfer(&map, &symbol, owner));
        assert_eq!(map.read().unwrap().get(&symbol), None);
    }

    #[test]
    fn exact_generation_removal_releases_restored_legacy_owner() {
        let map = make_in_progress();
        let symbol = Symbol::new("AAPL").unwrap();
        let generation = GuardGeneration::default();
        map.write()
            .unwrap()
            .insert(symbol.clone(), GuardState::ActiveTransfer { generation });

        assert!(remove_active_transfer(&map, &symbol, generation));
        assert_eq!(map.read().unwrap().get(&symbol), None);
    }

    #[test]
    fn exact_generation_removal_preserves_recovery_hold() {
        let map = make_in_progress();
        let symbol = Symbol::new("AAPL").unwrap();
        map.write()
            .unwrap()
            .insert(symbol.clone(), GuardState::HeldForRecovery);

        assert!(!remove_active_transfer(
            &map,
            &symbol,
            GuardGeneration::from_parts(NonZeroU32::new(7).unwrap(), 1)
        ));
        assert_eq!(
            map.read().unwrap().get(&symbol),
            Some(&GuardState::HeldForRecovery)
        );
    }

    #[test]
    fn test_guard_releases_on_drop() {
        let in_progress = make_in_progress();
        let symbol = Symbol::new("AAPL").unwrap();

        {
            let guard =
                InProgressGuard::try_claim_for_transfer(symbol.clone(), Arc::clone(&in_progress))
                    .unwrap();
            assert!(
                matches!(
                    in_progress.read().unwrap().get(&symbol),
                    Some(GuardState::ActiveTransfer { .. })
                ),
                "try_claim_for_transfer must insert ActiveTransfer"
            );
            drop(guard);
        }

        assert!(!in_progress.read().unwrap().contains_key(&symbol));
    }

    #[test]
    fn test_guard_defuse_prevents_release() {
        let in_progress = make_in_progress();
        let symbol = Symbol::new("AAPL").unwrap();

        {
            let guard =
                InProgressGuard::try_claim_for_transfer(symbol.clone(), Arc::clone(&in_progress))
                    .unwrap();
            assert!(in_progress.read().unwrap().contains_key(&symbol));
            guard.defuse();
        }

        // Should still be in progress after defused guard dropped.
        assert!(in_progress.read().unwrap().contains_key(&symbol));
    }

    #[test]
    fn test_guard_try_claim_fails_when_already_claimed() {
        let in_progress = make_in_progress();
        let symbol = Symbol::new("AAPL").unwrap();

        let _guard =
            InProgressGuard::try_claim_for_transfer(symbol.clone(), Arc::clone(&in_progress))
                .unwrap();

        let second_claim =
            InProgressGuard::try_claim_for_transfer(symbol, Arc::clone(&in_progress));
        assert!(second_claim.is_none());
    }

    #[test]
    fn guard_state_for_transfer_blocks_second_transfer_and_recovery_claim() {
        let in_progress = make_in_progress();
        let symbol = Symbol::new("AAPL").unwrap();

        let _guard =
            InProgressGuard::try_claim_for_transfer(symbol.clone(), Arc::clone(&in_progress))
                .unwrap();

        // A second transfer claim must fail.
        assert!(
            InProgressGuard::try_claim_for_transfer(symbol.clone(), Arc::clone(&in_progress))
                .is_none(),
            "ActiveTransfer must block a second transfer claim"
        );

        // A recovery claim must also fail when state is ActiveTransfer.
        assert!(
            claim_guard_for_recovery_or_orphan(&in_progress, &symbol).is_none(),
            "ActiveTransfer must block a recovery claim"
        );
    }

    #[test]
    fn guard_state_held_for_recovery_blocks_transfer_but_allows_recovery_claim() {
        let in_progress = make_in_progress();
        let symbol = Symbol::new("AAPL").unwrap();

        // Manually insert HeldForRecovery state (as mark_held_for_recovery would do).
        in_progress
            .write()
            .unwrap()
            .insert(symbol.clone(), GuardState::HeldForRecovery);

        // A transfer claim must fail -- no new transfer while recovery pending.
        assert!(
            InProgressGuard::try_claim_for_transfer(symbol.clone(), Arc::clone(&in_progress))
                .is_none(),
            "HeldForRecovery must block a transfer claim"
        );

        // A recovery claim via the production function must succeed.
        let guard = claim_guard_for_recovery_or_orphan(&in_progress, &symbol);
        let guard = guard.expect("HeldForRecovery must allow a recovery claim");
        drop(guard);
    }

    #[test]
    fn guard_state_held_for_recovery_transitions_to_active_on_recovery_claim() {
        let in_progress = make_in_progress();
        let symbol = Symbol::new("AAPL").unwrap();

        in_progress
            .write()
            .unwrap()
            .insert(symbol.clone(), GuardState::HeldForRecovery);

        let _guard = claim_guard_for_recovery_or_orphan(&in_progress, &symbol).unwrap();

        // After claiming for recovery, state transitions to ActiveTransfer.
        assert!(
            matches!(
                in_progress.read().unwrap().get(&symbol),
                Some(GuardState::ActiveTransfer { .. })
            ),
            "claim_guard_for_recovery_or_orphan must transition state to ActiveTransfer"
        );
    }

    /// When a recovery job claims from `HeldForRecovery` and then fails (drop
    /// without `release()`), the slot must be RESTORED to `HeldForRecovery` --
    /// not removed. This prevents a new transfer from starting while tokens are
    /// still stranded (double-mint safety guarantee).
    #[test]
    fn recovery_guard_drop_from_held_for_recovery_restores_held_for_recovery() {
        let map = make_in_progress();
        let symbol = Symbol::new("AAPL").unwrap();

        map.write()
            .unwrap()
            .insert(symbol.clone(), GuardState::HeldForRecovery);

        let guard = claim_guard_for_recovery_or_orphan(&map, &symbol)
            .expect("HeldForRecovery must allow claim");
        assert!(
            matches!(
                map.read().unwrap().get(&symbol),
                Some(GuardState::ActiveTransfer { .. })
            ),
            "claim must transition to ActiveTransfer"
        );

        // Simulate recovery failure: drop without release().
        drop(guard);

        // Must restore to HeldForRecovery (not absent, not ActiveTransfer).
        assert_eq!(
            map.read().unwrap().get(&symbol),
            Some(&GuardState::HeldForRecovery),
            "drop from HeldForRecovery claim must restore to HeldForRecovery, not remove"
        );

        // A subsequent recovery claim must still succeed (recovery can retry).
        let second_guard = claim_guard_for_recovery_or_orphan(&map, &symbol).expect(
            "a subsequent recovery claim must succeed after guard is restored to HeldForRecovery",
        );
        assert!(
            matches!(
                map.read().unwrap().get(&symbol),
                Some(GuardState::ActiveTransfer { .. })
            ),
            "second claim must transition to ActiveTransfer"
        );
        drop(second_guard);
    }

    /// ABA race: a terminal event clears the slot (absent), a NEW transfer
    /// immediately claims `ActiveTransfer`, and THEN an old `RecoveryGuard` drops.
    /// The old guard must detect the generation mismatch and NOT clobber the new
    /// transfer's claim.
    #[test]
    fn recovery_guard_drop_does_not_clobber_new_transfer_after_aba_race() {
        let map = make_in_progress();
        let symbol = Symbol::new("AAPL").unwrap();

        // Step 1: Recovery claims the slot from HeldForRecovery.
        map.write()
            .unwrap()
            .insert(symbol.clone(), GuardState::HeldForRecovery);
        let recovery_guard = claim_guard_for_recovery_or_orphan(&map, &symbol)
            .expect("HeldForRecovery must allow claim");

        // Step 2: Simulate terminal event clearing the slot (as if clear_equity_in_progress
        // fired -- e.g. the aggregate reached DepositedIntoRaindex).
        map.write().unwrap().remove(&symbol);

        // Step 3: A new transfer immediately claims ActiveTransfer (new generation).
        let new_transfer =
            InProgressGuard::try_claim_for_transfer(symbol.clone(), Arc::clone(&map))
                .expect("absent slot must allow new transfer after terminal event");
        let new_gen = match map.read().unwrap().get(&symbol) {
            Some(GuardState::ActiveTransfer { generation }) => *generation,
            other => panic!("expected ActiveTransfer after new claim, got {other:?}"),
        };

        // Step 4: Old recovery guard drops (ABA scenario). Must NOT remove or
        // overwrite the new transfer's claim.
        drop(recovery_guard);

        // The new transfer's ActiveTransfer entry must survive, unchanged.
        assert_eq!(
            map.read().unwrap().get(&symbol),
            Some(&GuardState::ActiveTransfer {
                generation: new_gen
            }),
            "ABA: old RecoveryGuard drop must not clobber the new transfer's guard claim"
        );

        // The new transfer's own drop cleans up normally.
        drop(new_transfer);
        assert!(
            !map.read().unwrap().contains_key(&symbol),
            "new transfer guard drop must remove the entry"
        );
    }

    /// An absent entry is treated as the orphan path: `claim_guard_for_recovery_or_orphan`
    /// returns `Some` and inserts `ActiveTransfer`, unblocking recovery without requiring
    /// a prior `HeldForRecovery` state.
    #[test]
    fn guard_absent_state_is_treated_as_orphan_by_recovery_claim() {
        let in_progress = make_in_progress();
        let symbol = Symbol::new("AAPL").unwrap();

        let _guard = claim_guard_for_recovery_or_orphan(&in_progress, &symbol)
            .expect("absent entry must be treated as orphan and allow a recovery claim");

        assert!(
            matches!(
                in_progress.read().unwrap().get(&symbol),
                Some(GuardState::ActiveTransfer { .. })
            ),
            "orphan claim from absent entry must insert ActiveTransfer"
        );
    }

    #[test]
    fn equity_rebalancing_check_label_includes_symbol() {
        let job = EquityRebalancingCheck {
            symbol: Symbol::new("RKLB").unwrap(),
        };
        assert_eq!(job.label().as_str(), "EquityRebalancingCheck(RKLB)");
    }

    async fn count_pending_equity_check_jobs(apalis_pool: &apalis_sqlite::SqlitePool) -> i64 {
        let job_type = std::any::type_name::<EquityRebalancingCheck>();
        sqlx_apalis::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM Jobs WHERE status = 'Pending' AND job_type = ?",
        )
        .bind(job_type)
        .fetch_one(apalis_pool)
        .await
        .expect("count pending equity-check jobs")
    }

    #[tokio::test]
    async fn equity_scheduler_enqueue_check_inserts_pending_row() {
        let apalis_pool = crate::test_utils::setup_test_apalis_pool().await;
        let scheduler = EquityRebalancingCheckScheduler::new(&apalis_pool);
        let symbol = Symbol::new("AAPL").unwrap();

        scheduler.enqueue_check(symbol).await;

        assert_eq!(count_pending_equity_check_jobs(&apalis_pool).await, 1);
    }

    #[tokio::test]
    async fn equity_scheduler_cancel_pending_marks_pending_rows_done() {
        let apalis_pool = crate::test_utils::setup_test_apalis_pool().await;
        let scheduler = EquityRebalancingCheckScheduler::new(&apalis_pool);

        scheduler.enqueue_check(Symbol::new("AAPL").unwrap()).await;
        scheduler.enqueue_check(Symbol::new("MSFT").unwrap()).await;
        assert_eq!(count_pending_equity_check_jobs(&apalis_pool).await, 2);

        scheduler.cancel_pending().await;

        assert_eq!(
            count_pending_equity_check_jobs(&apalis_pool).await,
            0,
            "cancel_pending must drain all pending rows"
        );
    }

    #[tokio::test]
    async fn equity_scheduler_enqueue_after_cancel_creates_fresh_row() {
        let apalis_pool = crate::test_utils::setup_test_apalis_pool().await;
        let scheduler = EquityRebalancingCheckScheduler::new(&apalis_pool);
        let symbol = Symbol::new("AAPL").unwrap();

        scheduler.enqueue_check(symbol.clone()).await;
        scheduler.cancel_pending().await;
        scheduler.enqueue_check(symbol).await;

        assert_eq!(count_pending_equity_check_jobs(&apalis_pool).await, 1);
    }
}

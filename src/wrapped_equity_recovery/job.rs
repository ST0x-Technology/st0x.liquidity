//! Apalis job that drives wrapped-equity recovery.
//!
//! Enqueued per symbol with a positive `BaseWalletWrappedEquity` balance.
//! The job:
//!
//! 1. Tries to claim the `equity_in_progress` guard for the symbol. If
//!    another task already holds it the job exits at debug log; the next
//!    inventory poll re-evaluates.
//! 2. Consults the inventory view's active mint / active redemption maps
//!    to decide which path applies.
//! 3. Initializes a `WrappedEquityRecovery` aggregate via the supplied
//!    `Store`, records the dispatch decision, and -- for the orphan path
//!    -- drives the deposit through to `RecoveryCompleted`.

use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use st0x_event_sorcery::{SendError, Store};
use st0x_execution::{FractionalShares, SharesBlockchain, SharesConversionError, Symbol};

#[cfg(any(test, feature = "test-support"))]
use crate::conductor::job::JobKind;
use crate::conductor::job::{Job, JobQueue, Label};
use crate::equity_redemption::RedemptionAggregateId;
use crate::inventory::BroadcastingInventory;
use crate::inventory::view::{InFlightEquityLocation, InventoryView};
use crate::onchain::raindex::{Raindex, RaindexError};
use crate::rebalancing::equity::{CrossVenueEquityTransfer, MintError, RedemptionError};
use crate::tokenized_equity_mint::{IssuerRequestId, TOKENIZED_EQUITY_DECIMALS};
use crate::wrapper::{Wrapper, WrapperError};

use super::aggregate::{
    RecoveryOutcome, WrappedEquityRecovery, WrappedEquityRecoveryCommand,
    WrappedEquityRecoveryError, WrappedEquityRecoveryId,
};

/// Apalis queue type for [`WrappedEquityRecoveryJob`].
pub(crate) type WrappedEquityRecoveryJobQueue = JobQueue<WrappedEquityRecoveryJob>;

/// Dependencies the recovery job needs to look up active aggregates,
/// claim the per-symbol concurrency guard, drive existing transfers
/// forward, and emit aggregate commands.
pub(crate) struct WrappedEquityRecoveryCtx {
    /// Snapshot of the current inventory view, used to read the
    /// `active_mints` / `active_redemptions` maps and the wallet balance.
    pub(crate) inventory: Arc<BroadcastingInventory>,
    /// Aggregate store for emitting recovery events.
    pub(crate) store: Arc<Store<WrappedEquityRecovery>>,
    /// Drives the underlying mint / redemption forward when an active
    /// aggregate exists.
    pub(crate) transfer: Arc<CrossVenueEquityTransfer>,
    /// Shared with the rebalancing trigger so a recovery dispatch can't
    /// race a normal transfer for the same symbol.
    pub(crate) equity_in_progress: Arc<RwLock<HashSet<Symbol>>>,
    /// Raindex service for the orphan deposit path.
    pub(crate) raindex: Arc<dyn Raindex>,
    /// Wrapper service for symbol -> wtSTOCK address resolution.
    pub(crate) wrapper: Arc<dyn Wrapper>,
}

/// Why a single recovery job attempt failed. Errors propagate up through
/// apalis; the aggregate also records a `RecoveryFailed` event with the
/// rendered reason.
#[derive(Debug, Error)]
pub(crate) enum WrappedEquityRecoveryJobError {
    #[error("recovery aggregate error: {0}")]
    Aggregate(#[from] SendError<WrappedEquityRecovery>),

    #[error(transparent)]
    Domain(#[from] WrappedEquityRecoveryError),

    #[error("raindex error: {0}")]
    Raindex(#[from] RaindexError),

    #[error("wrapper error: {0}")]
    Wrapper(#[from] WrapperError),

    #[error("shares conversion error: {0}")]
    Shares(#[from] SharesConversionError),

    #[error("failed to resume mint: {0}")]
    ResumeMint(#[from] MintError),

    #[error("failed to resume redemption: {0}")]
    ResumeRedemption(#[from] RedemptionError),
}

/// Apalis job payload. One job per symbol-with-positive-wallet-balance,
/// pushed by the rebalancing reactor on every `BaseWalletWrappedEquity`
/// snapshot event.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct WrappedEquityRecoveryJob {
    pub(crate) symbol: Symbol,
}

impl Job<WrappedEquityRecoveryCtx> for WrappedEquityRecoveryJob {
    type Output = ();
    type Error = WrappedEquityRecoveryJobError;

    const WORKER_NAME: &'static str = "wrapped-equity-recovery-worker";

    #[cfg(any(test, feature = "test-support"))]
    const JOB_KIND: JobKind = JobKind::WrappedEquityRecovery;

    fn label(&self) -> Label {
        Label::new(format!("WrappedEquityRecovery:{}", self.symbol))
    }

    async fn perform(&self, ctx: &WrappedEquityRecoveryCtx) -> Result<Self::Output, Self::Error> {
        let symbol = self.symbol.clone();

        let Some(guard) = claim_guard(&ctx.equity_in_progress, &symbol) else {
            debug!(
                target: "rebalance",
                %symbol,
                "Skipping wrapped equity recovery: equity_in_progress already held",
            );
            return Ok(());
        };

        let snapshot = read_recovery_snapshot(&ctx.inventory, &symbol).await;
        let Some(snapshot) = snapshot else {
            debug!(
                target: "rebalance",
                %symbol,
                "Skipping wrapped equity recovery: no positive balance in inventory view",
            );
            drop(guard);
            return Ok(());
        };

        let recovery_id = WrappedEquityRecoveryId(Uuid::new_v4());

        ctx.store
            .send(
                &recovery_id,
                WrappedEquityRecoveryCommand::Detect {
                    symbol: symbol.clone(),
                    shares: snapshot.shares,
                },
            )
            .await?;

        info!(
            target: "rebalance",
            %symbol,
            %recovery_id,
            shares = %snapshot.shares,
            "Wrapped equity recovery: dispatched detection",
        );

        let outcome = match snapshot.dispatch {
            DispatchDecision::ActiveMint(mint_id) => {
                dispatch_mint(ctx, &recovery_id, &mint_id).await
            }
            DispatchDecision::ActiveRedemption(redemption_id) => {
                dispatch_redemption(ctx, &recovery_id, &redemption_id).await
            }
            DispatchDecision::Orphan => {
                dispatch_orphan(ctx, &recovery_id, &symbol, snapshot.shares).await
            }
        };

        match outcome {
            Ok(outcome) => {
                ctx.store
                    .send(
                        &recovery_id,
                        WrappedEquityRecoveryCommand::CompleteRecovery { outcome },
                    )
                    .await?;
                info!(target: "rebalance", %symbol, %recovery_id, "Wrapped equity recovery completed");
            }
            Err(error) => {
                let reason = format!("{error}");
                error!(
                    target: "rebalance",
                    %symbol,
                    %recovery_id,
                    %reason,
                    "Wrapped equity recovery failed",
                );
                ctx.store
                    .send(
                        &recovery_id,
                        WrappedEquityRecoveryCommand::FailRecovery { reason },
                    )
                    .await?;
                return Err(error);
            }
        }

        drop(guard);
        Ok(())
    }
}

struct RecoverySnapshot {
    shares: FractionalShares,
    dispatch: DispatchDecision,
}

enum DispatchDecision {
    ActiveMint(IssuerRequestId),
    ActiveRedemption(RedemptionAggregateId),
    Orphan,
}

async fn read_recovery_snapshot(
    inventory: &BroadcastingInventory,
    symbol: &Symbol,
) -> Option<RecoverySnapshot> {
    let view = inventory.read().await;
    let shares = view.inflight_equity_at(symbol, InFlightEquityLocation::BaseWalletWrapped)?;
    if shares == FractionalShares::ZERO {
        return None;
    }
    let dispatch = decide_dispatch(&view, symbol);
    drop(view);
    Some(RecoverySnapshot { shares, dispatch })
}

fn decide_dispatch(view: &InventoryView, symbol: &Symbol) -> DispatchDecision {
    view.active_mint(symbol).map_or_else(
        || {
            view.active_redemption(symbol)
                .map_or(DispatchDecision::Orphan, |redemption_id| {
                    DispatchDecision::ActiveRedemption(redemption_id.clone())
                })
        },
        |mint_id| DispatchDecision::ActiveMint(mint_id.clone()),
    )
}

/// RAII helper that inserts `symbol` into `equity_in_progress` and removes it
/// on drop. Returns `None` if another task already holds the guard.
struct InProgressGuard {
    symbol: Symbol,
    set: Arc<RwLock<HashSet<Symbol>>>,
}

impl Drop for InProgressGuard {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.set.write() {
            guard.remove(&self.symbol);
        }
    }
}

fn claim_guard(set: &Arc<RwLock<HashSet<Symbol>>>, symbol: &Symbol) -> Option<InProgressGuard> {
    let mut guard = set.write().ok()?;
    if guard.contains(symbol) {
        return None;
    }
    guard.insert(symbol.clone());
    drop(guard);
    Some(InProgressGuard {
        symbol: symbol.clone(),
        set: Arc::clone(set),
    })
}

async fn dispatch_mint(
    ctx: &WrappedEquityRecoveryCtx,
    recovery_id: &WrappedEquityRecoveryId,
    mint_id: &IssuerRequestId,
) -> Result<RecoveryOutcome, WrappedEquityRecoveryJobError> {
    ctx.store
        .send(
            recovery_id,
            WrappedEquityRecoveryCommand::DispatchToMint {
                mint_id: mint_id.clone(),
            },
        )
        .await?;

    ctx.transfer
        .resume_mint(mint_id)
        .await
        .inspect_err(|error| {
            warn!(
                target: "rebalance",
                %mint_id,
                ?error,
                "Wrapped equity recovery: resume_mint failed",
            );
        })?;

    Ok(RecoveryOutcome::MintResumed {
        mint_id: mint_id.clone(),
    })
}

async fn dispatch_redemption(
    ctx: &WrappedEquityRecoveryCtx,
    recovery_id: &WrappedEquityRecoveryId,
    redemption_id: &RedemptionAggregateId,
) -> Result<RecoveryOutcome, WrappedEquityRecoveryJobError> {
    ctx.store
        .send(
            recovery_id,
            WrappedEquityRecoveryCommand::DispatchToRedemption {
                redemption_id: redemption_id.clone(),
            },
        )
        .await?;

    ctx.transfer
        .resume_redemption(redemption_id)
        .await
        .inspect_err(|error| {
            warn!(
                target: "rebalance",
                %redemption_id,
                ?error,
                "Wrapped equity recovery: resume_redemption failed",
            );
        })?;

    Ok(RecoveryOutcome::RedemptionResumed {
        redemption_id: redemption_id.clone(),
    })
}

async fn dispatch_orphan(
    ctx: &WrappedEquityRecoveryCtx,
    recovery_id: &WrappedEquityRecoveryId,
    symbol: &Symbol,
    shares: FractionalShares,
) -> Result<RecoveryOutcome, WrappedEquityRecoveryJobError> {
    let wrapped_token = ctx.wrapper.lookup_derivative(symbol)?;
    let vault_id = ctx.raindex.lookup_vault_id(wrapped_token).await?;
    let raw = shares.to_u256_18_decimals()?;

    let tx_hash = ctx
        .raindex
        .submit_deposit(wrapped_token, vault_id, raw, TOKENIZED_EQUITY_DECIMALS)
        .await?;

    ctx.store
        .send(
            recovery_id,
            WrappedEquityRecoveryCommand::SubmitOrphanDeposit {
                vault_deposit_tx_hash: tx_hash,
            },
        )
        .await?;

    ctx.raindex.confirm_tx(tx_hash).await?;

    ctx.store
        .send(
            recovery_id,
            WrappedEquityRecoveryCommand::ConfirmOrphanDeposit {
                vault_deposit_tx_hash: tx_hash,
            },
        )
        .await?;

    Ok(RecoveryOutcome::OrphanDeposited {
        vault_deposit_tx_hash: tx_hash,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use alloy::primitives::{Address, TxHash, U256};
    use chrono::Utc;
    use rain_math_float::Float;
    use sqlx::SqlitePool;
    use tokio::sync::broadcast;

    use st0x_event_sorcery::test_store;

    use crate::equity_redemption::{EquityRedemption, EquityRedemptionCommand};
    use crate::onchain::mock::MockRaindex;
    use crate::onchain::raindex::Raindex;
    use crate::rebalancing::equity::EquityTransferServices;
    use crate::tokenization::Tokenizer;
    use crate::tokenization::mock::{MockCompletionOutcome, MockDetectionOutcome, MockTokenizer};
    use crate::tokenized_equity_mint::{TokenizedEquityMint, TokenizedEquityMintCommand};
    use crate::wrapper::mock::MockWrapper;

    use super::*;

    fn aapl() -> Symbol {
        Symbol::new("AAPL").unwrap()
    }

    fn one_share() -> FractionalShares {
        FractionalShares::new(Float::parse("1".to_string()).unwrap())
    }

    fn view_with_balance(symbol: Symbol, shares: FractionalShares) -> InventoryView {
        let mut balances = BTreeMap::new();
        balances.insert(symbol, shares);
        let now = Utc::now();
        InventoryView::default().set_inflight_equity_at_location(
            InFlightEquityLocation::BaseWalletWrapped,
            &balances,
            now,
            now,
        )
    }

    /// Handles surfaced from [`setup_ctx`] so tests can seed underlying
    /// mint/redemption aggregates before running the recovery job.
    struct CtxHandles {
        ctx: WrappedEquityRecoveryCtx,
        pool: SqlitePool,
        mint_store: Arc<Store<TokenizedEquityMint>>,
        redemption_store: Arc<Store<EquityRedemption>>,
    }

    async fn setup_ctx(view: InventoryView) -> CtxHandles {
        setup_ctx_with_tokenizer(view, Arc::new(MockTokenizer::new())).await
    }

    async fn setup_ctx_with_tokenizer(
        view: InventoryView,
        tokenizer: Arc<dyn Tokenizer>,
    ) -> CtxHandles {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();

        let (sender, _receiver) = broadcast::channel(16);
        let inventory = Arc::new(BroadcastingInventory::new(view, sender));

        let raindex = Arc::new(MockRaindex::new());
        let wrapper = Arc::new(MockWrapper::new());

        let services = EquityTransferServices {
            raindex: raindex.clone(),
            tokenizer: tokenizer.clone(),
            wrapper: wrapper.clone(),
        };

        let mint_store = Arc::new(test_store::<TokenizedEquityMint>(
            pool.clone(),
            services.clone(),
        ));
        let redemption_store = Arc::new(test_store::<EquityRedemption>(pool.clone(), services));

        let transfer = Arc::new(CrossVenueEquityTransfer::new(
            raindex.clone(),
            tokenizer,
            wrapper.clone(),
            Address::random(),
            mint_store.clone(),
            redemption_store.clone(),
        ));

        let store = Arc::new(test_store::<WrappedEquityRecovery>(pool.clone(), ()));

        let ctx = WrappedEquityRecoveryCtx {
            inventory,
            store,
            transfer,
            equity_in_progress: Arc::new(RwLock::new(HashSet::new())),
            raindex,
            wrapper,
        };
        CtxHandles {
            ctx,
            pool,
            mint_store,
            redemption_store,
        }
    }

    async fn fetch_recovery_event_types(pool: &SqlitePool) -> Vec<String> {
        sqlx::query_scalar::<_, String>(
            "SELECT event_type FROM events \
             WHERE aggregate_type = 'WrappedEquityRecovery' \
             ORDER BY sequence",
        )
        .fetch_all(pool)
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn orphan_path_drives_recovery_through_raindex_deposit() {
        let symbol = aapl();
        let view = view_with_balance(symbol.clone(), one_share());
        let CtxHandles { ctx, pool, .. } = setup_ctx(view).await;

        WrappedEquityRecoveryJob { symbol }
            .perform(&ctx)
            .await
            .unwrap();

        assert_eq!(
            fetch_recovery_event_types(&pool).await,
            vec![
                "WrappedEquityRecoveryEvent::Detected",
                "WrappedEquityRecoveryEvent::OrphanDepositSubmitted",
                "WrappedEquityRecoveryEvent::OrphanDeposited",
                "WrappedEquityRecoveryEvent::RecoveryCompleted",
            ],
        );
    }

    /// Active mint case: the inventory view names a specific mint id that
    /// recovery must load via `Store::load(id)` and resume. We don't seed the
    /// mint here, so `resume_mint` fails with `EntityNotFound{mint_id}` --
    /// proving the dispatch routed through `resume_mint` with the id from the
    /// inventory view (a wrong id or wrong dispatch path would produce a
    /// different error).
    #[tokio::test]
    async fn active_mint_dispatches_resume_mint_with_inventory_id() {
        let symbol = aapl();
        let mint_id = IssuerRequestId::new("ISS-RECOVERY-TEST");
        let view = view_with_balance(symbol.clone(), one_share())
            .set_active_mint(symbol.clone(), mint_id.clone());
        let CtxHandles { ctx, pool, .. } = setup_ctx(view).await;

        let error = WrappedEquityRecoveryJob { symbol }
            .perform(&ctx)
            .await
            .expect_err("resume_mint should fail when no mint exists in the store");

        assert!(
            matches!(
                error,
                WrappedEquityRecoveryJobError::ResumeMint(MintError::EntityNotFound {
                    issuer_request_id: ref id,
                    ..
                }) if id == &mint_id,
            ),
            "expected ResumeMint(EntityNotFound) carrying the inventory mint id; got {error:?}",
        );

        assert_eq!(
            fetch_recovery_event_types(&pool).await,
            vec![
                "WrappedEquityRecoveryEvent::Detected",
                "WrappedEquityRecoveryEvent::DispatchedToMint",
                "WrappedEquityRecoveryEvent::RecoveryFailed",
            ],
        );
    }

    /// Active redemption mirror of the mint case: the inventory view names a
    /// specific redemption id; resume_redemption fails because the redemption
    /// is not seeded, surfacing as the typed `ResumeRedemption(EntityNotFound)`
    /// variant rather than an opaque string wrap.
    #[tokio::test]
    async fn active_redemption_dispatches_resume_redemption_with_inventory_id() {
        let symbol = aapl();
        let redemption_id = RedemptionAggregateId("redemption-recovery-test".to_string());
        let view = view_with_balance(symbol.clone(), one_share())
            .set_active_redemption(symbol.clone(), redemption_id.clone());
        let CtxHandles { ctx, pool, .. } = setup_ctx(view).await;

        let error = WrappedEquityRecoveryJob { symbol }
            .perform(&ctx)
            .await
            .expect_err("resume_redemption should fail when no redemption exists");

        assert!(
            matches!(
                error,
                WrappedEquityRecoveryJobError::ResumeRedemption(
                    RedemptionError::EntityNotFound { aggregate_id: ref id },
                ) if id == &redemption_id,
            ),
            "expected ResumeRedemption(EntityNotFound) carrying the inventory id; got {error:?}",
        );

        assert_eq!(
            fetch_recovery_event_types(&pool).await,
            vec![
                "WrappedEquityRecoveryEvent::Detected",
                "WrappedEquityRecoveryEvent::DispatchedToRedemption",
                "WrappedEquityRecoveryEvent::RecoveryFailed",
            ],
        );
    }

    #[tokio::test]
    async fn empty_wallet_balance_skips_recovery() {
        let CtxHandles { ctx, pool, .. } = setup_ctx(InventoryView::default()).await;

        WrappedEquityRecoveryJob { symbol: aapl() }
            .perform(&ctx)
            .await
            .unwrap();

        assert!(
            fetch_recovery_event_types(&pool).await.is_empty(),
            "no inventory balance -> no recovery aggregate",
        );
    }

    /// Seeds a `TokenizedEquityMint` at `TokensWrapped` via the same direct
    /// command sequence as `resume_mint_recovers_when_deposit_reverts`. The
    /// MockTokenizer's default `Completed` poll outcome drives the mint from
    /// `MintRequested` through `MintAccepted` to `TokensReceived` on `Poll`.
    async fn seed_mint_at_tokens_wrapped(
        mint_store: &Store<TokenizedEquityMint>,
        mint_id: &IssuerRequestId,
        symbol: &Symbol,
    ) {
        mint_store
            .send(
                mint_id,
                TokenizedEquityMintCommand::RequestMint {
                    issuer_request_id: mint_id.clone(),
                    symbol: symbol.clone(),
                    quantity: Float::parse("10".to_string()).unwrap(),
                    wallet: Address::random(),
                },
            )
            .await
            .unwrap();

        mint_store
            .send(mint_id, TokenizedEquityMintCommand::Poll)
            .await
            .unwrap();

        let wrap_tx = TxHash::random();
        mint_store
            .send(
                mint_id,
                TokenizedEquityMintCommand::SubmitWrap {
                    wrap_tx_hash: wrap_tx,
                },
            )
            .await
            .unwrap();

        mint_store
            .send(
                mint_id,
                TokenizedEquityMintCommand::WrapTokens {
                    wrap_tx_hash: wrap_tx,
                    wrapped_shares: U256::from(10_000_000_000_000_000_000u128),
                },
            )
            .await
            .unwrap();

        let entity = mint_store.load(mint_id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::TokensWrapped { .. }),
            "mint seeding precondition: expected TokensWrapped, got {entity:?}",
        );
    }

    /// Success path: with an active mint stuck at `TokensWrapped`, the recovery
    /// job must drive it through the deposit and the mint must end at
    /// `DepositedIntoRaindex`.
    #[tokio::test]
    async fn active_mint_in_tokens_wrapped_recovers_to_deposited_into_raindex() {
        let symbol = aapl();
        let mint_id = IssuerRequestId::new("ISS-RECOVERY-SUCCESS");
        let view = view_with_balance(symbol.clone(), one_share())
            .set_active_mint(symbol.clone(), mint_id.clone());

        let handles = setup_ctx(view).await;
        seed_mint_at_tokens_wrapped(&handles.mint_store, &mint_id, &symbol).await;

        WrappedEquityRecoveryJob {
            symbol: symbol.clone(),
        }
        .perform(&handles.ctx)
        .await
        .unwrap();

        let mint_state = handles.mint_store.load(&mint_id).await.unwrap().unwrap();
        assert!(
            matches!(mint_state, TokenizedEquityMint::DepositedIntoRaindex { .. }),
            "recovery should deposit the wtSTOCK into Raindex; got {mint_state:?}",
        );

        assert_eq!(
            fetch_recovery_event_types(&handles.pool).await,
            vec![
                "WrappedEquityRecoveryEvent::Detected",
                "WrappedEquityRecoveryEvent::DispatchedToMint",
                "WrappedEquityRecoveryEvent::RecoveryCompleted",
            ],
        );
    }

    /// Seeds an `EquityRedemption` at `WithdrawnFromRaindex`. Mirrors the
    /// command sequence in `withdraw_from_raindex` (`Redeem` ->
    /// `SubmitWithdraw` -> `ConfirmWithdraw`).
    async fn seed_redemption_at_withdrawn_from_raindex(
        redemption_store: &Store<EquityRedemption>,
        raindex: &MockRaindex,
        redemption_id: &RedemptionAggregateId,
        symbol: &Symbol,
    ) {
        let quantity = FractionalShares::new(Float::parse("10".to_string()).unwrap());
        let amount = quantity.to_u256_18_decimals().unwrap();
        let (token, _vault) = raindex.lookup_vault_info(symbol).await.unwrap();

        redemption_store
            .send(
                redemption_id,
                EquityRedemptionCommand::Redeem {
                    symbol: symbol.clone(),
                    quantity: quantity.inner(),
                    token,
                    amount,
                },
            )
            .await
            .unwrap();

        redemption_store
            .send(redemption_id, EquityRedemptionCommand::SubmitWithdraw)
            .await
            .unwrap();

        redemption_store
            .send(redemption_id, EquityRedemptionCommand::ConfirmWithdraw)
            .await
            .unwrap();

        let entity = redemption_store.load(redemption_id).await.unwrap().unwrap();
        assert!(
            matches!(entity, EquityRedemption::WithdrawnFromRaindex { .. }),
            "redemption seeding precondition: expected WithdrawnFromRaindex, got {entity:?}",
        );
    }

    /// Success path: with an active redemption stuck at `WithdrawnFromRaindex`,
    /// the recovery job must dispatch through `resume_redemption` and progress
    /// the redemption past the unwrap step.
    #[tokio::test]
    async fn active_redemption_in_withdrawn_from_raindex_recovers_via_unwrap() {
        let symbol = aapl();
        let redemption_id = RedemptionAggregateId("redemption-recovery-success".to_string());

        let tokenizer: Arc<dyn Tokenizer> = Arc::new(
            MockTokenizer::new()
                .with_detection_outcome(MockDetectionOutcome::Detected)
                .with_completion_outcome(MockCompletionOutcome::Completed),
        );
        let view = view_with_balance(symbol.clone(), one_share())
            .set_active_redemption(symbol.clone(), redemption_id.clone());

        let handles = setup_ctx_with_tokenizer(view, tokenizer).await;

        // Reuse the same MockRaindex defaults the ctx uses for the seeding
        // lookup -- the token address returned here matches what the ctx's
        // raindex will return inside `resume_redemption`.
        seed_redemption_at_withdrawn_from_raindex(
            &handles.redemption_store,
            &MockRaindex::new(),
            &redemption_id,
            &symbol,
        )
        .await;

        WrappedEquityRecoveryJob {
            symbol: symbol.clone(),
        }
        .perform(&handles.ctx)
        .await
        .unwrap();

        let redemption_state = handles
            .redemption_store
            .load(&redemption_id)
            .await
            .unwrap()
            .unwrap();
        assert!(
            !matches!(
                redemption_state,
                EquityRedemption::WithdrawnFromRaindex { .. },
            ),
            "recovery should drive the redemption past WithdrawnFromRaindex; \
             still at {redemption_state:?}",
        );

        assert_eq!(
            fetch_recovery_event_types(&handles.pool).await,
            vec![
                "WrappedEquityRecoveryEvent::Detected",
                "WrappedEquityRecoveryEvent::DispatchedToRedemption",
                "WrappedEquityRecoveryEvent::RecoveryCompleted",
            ],
        );
    }
}

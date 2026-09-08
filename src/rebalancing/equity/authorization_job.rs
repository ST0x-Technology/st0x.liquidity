//! Apalis job that delivers a signed MintAuthV1 recipient authorization to
//! the issuance bot for an orchestrator-mode mint (RAI-1243).
//!
//! Enqueued (idempotently, keyed on the issuer request id) right after
//! `MintAuthorizationSigned` persists. The payload carries only the
//! aggregate identity: the tokenization request id and the signed
//! `{nonce, signature}` are loaded from the aggregate at `perform` time, so
//! every attempt redelivers the PERSISTED authorization byte-identically --
//! issuance treats an identical redelivery as an idempotent `200`, and the
//! nonce is the mint's on-chain idempotency key, so this job never signs.
//!
//! Outcome handling: `Recorded` dispatches
//! `RecordMintAuthorizationDelivered`; the retryable classifications
//! (issuance has no mint yet -- the initiation race -- or a transport
//! failure) re-enqueue a delayed successor instead of consuming the
//! three-attempt apalis retry budget -- a bounded fast cadence, then one
//! operator alert and a slow cadence that keeps the chain alive until
//! issuance accepts; the non-retryable rejections (`409`/`422`, unexpected
//! `4xx`) park the mint with a single operator alert.
//! Runs on a best-effort worker: a stuck authorization must never halt
//! hedging or fill detection.

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{error, info, warn};

use st0x_event_sorcery::{SendError, Store};
use st0x_tokenization::{IssuerRequestId, TokenizationRequestId};

use crate::alerts::Notifier;
use crate::conductor::job::{Job, JobQueue, Label, QueuePushError};
use crate::mint_authorization::{
    MintAuthorizationDeliverer, MintAuthorizationDelivery, SignedMintAuthorization,
};
use crate::tokenized_equity_mint::{
    MintAuthorizationProgress, TokenizedEquityMint, TokenizedEquityMintCommand,
};

/// Apalis queue type for [`DeliverMintAuthorization`].
pub(crate) type DeliverMintAuthorizationJobQueue = JobQueue<DeliverMintAuthorization>;

/// Delay before re-enqueueing after a retryable delivery outcome. Long
/// enough to ride out the Alpaca->issuance initiation race (the usual cause
/// of `MintNotFoundYet`) and transient issuance unavailability.
const DELIVERY_REDRIVE_DELAY: Duration = Duration::from_secs(30);

/// Bounded fast-redrive budget (~10 minutes at the delay above). At the
/// limit the job alerts once and degrades to
/// [`DELIVERY_SLOW_REDRIVE_DELAY`]: an issuance service unreachable for
/// that long needs a human anyway, but the chain must stay alive -- the
/// initial enqueue's idempotency key survives in the finished row and
/// blocks a replacement chain until cleanup prunes it, so dead-lettering
/// here would strand the mint instead of letting it complete on its own
/// once issuance recovers.
const MAX_DELIVERY_REDRIVES: u32 = 20;

/// Cadence past the fast budget: slow enough to be negligible load on a
/// struggling issuance service, frequent enough that delivery completes
/// within minutes of it recovering, with no restart required.
const DELIVERY_SLOW_REDRIVE_DELAY: Duration = Duration::from_secs(600);

/// Apalis job payload. Carries only the aggregate identity; the
/// authorization itself is loaded from the aggregate at `perform` time.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct DeliverMintAuthorization {
    pub(crate) issuer_request_id: IssuerRequestId,
    /// Count of consecutive delayed redrives leading up to this attempt.
    /// `#[serde(default)]` so a row enqueued under an older payload shape
    /// still deserializes.
    #[serde(default)]
    pub(crate) redrive_attempts: u32,
}

/// Dependencies the job needs.
pub(crate) struct DeliverMintAuthorizationCtx {
    pub(crate) deliverer: Arc<dyn MintAuthorizationDeliverer>,
    pub(crate) mint_store: Arc<Store<TokenizedEquityMint>>,
    /// Operator alerting for the park outcomes (structured
    /// operational-alert logs in production).
    pub(crate) notifier: Arc<dyn Notifier>,
    /// For delayed self-redrives on retryable outcomes.
    pub(crate) job_queue: DeliverMintAuthorizationJobQueue,
}

/// Errors emitted by [`DeliverMintAuthorization::perform`].
#[derive(Debug, Error)]
pub(crate) enum DeliverMintAuthorizationError {
    #[error(transparent)]
    Mint(#[from] Box<SendError<TokenizedEquityMint>>),
    #[error(transparent)]
    QueuePush(#[from] QueuePushError),
    /// The delivery job fired for an aggregate that does not exist -- the
    /// job is only ever enqueued after the aggregate persisted its signed
    /// authorization, so this is a caller bug, not a race.
    #[error("no mint aggregate found for {issuer_request_id}")]
    MintNotFound { issuer_request_id: IssuerRequestId },
}

impl From<SendError<TokenizedEquityMint>> for DeliverMintAuthorizationError {
    fn from(error: SendError<TokenizedEquityMint>) -> Self {
        Self::Mint(Box::new(error))
    }
}

impl Job<DeliverMintAuthorizationCtx> for DeliverMintAuthorization {
    type Output = ();
    type Error = DeliverMintAuthorizationError;

    const WORKER_NAME: &'static str = "deliver-mint-authorization-worker";
    const PERFORM_TIMEOUT: Option<std::time::Duration> =
        Some(crate::conductor::job::DEFAULT_PERFORM_TIMEOUT);
    const TERMINAL_FAILURE_MSG: &'static str = "Mint authorization delivery failed all retries; the orchestrator-mode \
         mint cannot proceed until the authorization reaches issuance. \
         Operator action required.";

    #[cfg(any(test, feature = "test-support"))]
    const JOB_KIND: crate::conductor::job::JobKind =
        crate::conductor::job::JobKind::DeliverMintAuthorization;

    fn label(&self) -> Label {
        Label::new(format!(
            "DeliverMintAuthorization:{}",
            self.issuer_request_id
        ))
    }

    async fn perform(&self, ctx: &DeliverMintAuthorizationCtx) -> Result<(), Self::Error> {
        let Some(mint) = ctx.mint_store.load(&self.issuer_request_id).await? else {
            return Err(DeliverMintAuthorizationError::MintNotFound {
                issuer_request_id: self.issuer_request_id.clone(),
            });
        };

        let Some((tokenization_request_id, authorization)) = awaiting_delivery(&mint) else {
            info!(
                target: "tokenization",
                issuer_request_id = %self.issuer_request_id,
                "Mint no longer awaiting authorization delivery; nothing to do"
            );
            return Ok(());
        };

        match ctx
            .deliverer
            .deliver(&tokenization_request_id, &authorization)
            .await
        {
            MintAuthorizationDelivery::Recorded => {
                ctx.mint_store
                    .send(
                        &self.issuer_request_id,
                        TokenizedEquityMintCommand::RecordMintAuthorizationDelivered,
                    )
                    .await
                    .map_err(Box::new)?;
                Ok(())
            }
            MintAuthorizationDelivery::MintNotFoundYet => {
                self.redrive(ctx, "issuance has no mint for the tokenization request yet")
                    .await
            }
            MintAuthorizationDelivery::RetryableFailure(failure) => {
                self.redrive(ctx, &format!("issuance request failed: {failure}"))
                    .await
            }
            MintAuthorizationDelivery::Conflict => {
                self.park_with_alert(
                    ctx,
                    "issuance reports a conflicting authorization (409): either a \
                     different authorization is already recorded for this mint, or \
                     its transaction already binds a nonce",
                )
                .await
            }
            MintAuthorizationDelivery::Rejected => {
                self.park_with_alert(
                    ctx,
                    "issuance rejected the authorization (422): vault-direct mint, \
                     signer mismatch, or the nonce is already consumed on-chain",
                )
                .await
            }
            MintAuthorizationDelivery::PermanentFailure(status) => {
                self.park_with_alert(
                    ctx,
                    &format!(
                        "issuance returned a permanent client failure ({status}): \
                         redelivering the identical request cannot succeed \
                         (defective request or credentials)"
                    ),
                )
                .await
            }
        }
    }
}

impl DeliverMintAuthorization {
    /// Re-enqueues a delayed successor: [`DELIVERY_REDRIVE_DELAY`] within
    /// the fast budget, then one operator alert and
    /// [`DELIVERY_SLOW_REDRIVE_DELAY`] from there on. The chain never
    /// dead-letters: its live row is what the one-live-chain guard keys on,
    /// and the initial enqueue's idempotency key blocks a replacement
    /// chain, so killing the chain would strand the mint until manual
    /// action. Uses delayed redrives rather than `Err` so a slow issuance
    /// start-up cannot burn the three-attempt apalis budget in seconds (or
    /// repeat the boundary alert on every apalis retry).
    async fn redrive(
        &self,
        ctx: &DeliverMintAuthorizationCtx,
        reason: &str,
    ) -> Result<(), DeliverMintAuthorizationError> {
        let attempts = self.redrive_attempts + 1;

        if attempts == MAX_DELIVERY_REDRIVES + 1 {
            error!(
                target: "tokenization",
                issuer_request_id = %self.issuer_request_id,
                attempts,
                reason,
                "Mint authorization delivery exhausted its fast-redrive \
                 budget; degrading to slow redrives"
            );
            notify_swallowing_failure(
                ctx,
                &format!(
                    "Mint authorization delivery for {} failed {} consecutive \
                     retries ({reason}). It keeps redelivering every {} minutes \
                     and completes automatically once issuance accepts; \
                     operator attention needed if this persists.",
                    self.issuer_request_id,
                    MAX_DELIVERY_REDRIVES,
                    DELIVERY_SLOW_REDRIVE_DELAY.as_secs() / 60
                ),
            )
            .await;
        }

        let delay = if attempts > MAX_DELIVERY_REDRIVES {
            DELIVERY_SLOW_REDRIVE_DELAY
        } else {
            DELIVERY_REDRIVE_DELAY
        };

        warn!(
            target: "tokenization",
            issuer_request_id = %self.issuer_request_id,
            attempt = attempts,
            reason,
            "Re-enqueueing mint authorization delivery"
        );

        let mut queue = ctx.job_queue.clone();
        queue
            .push_with_delay(
                Self {
                    issuer_request_id: self.issuer_request_id.clone(),
                    redrive_attempts: attempts,
                },
                delay,
            )
            .await?;

        Ok(())
    }

    /// Parks a non-retryable rejection: one loud error, one operator alert,
    /// then `Ok` so the best-effort worker keeps running. The mint stays in
    /// `MintAccepted` with its authorization `Signed`; resolution is manual
    /// (issuance's admin surface owns the conflicting state).
    async fn park_with_alert(
        &self,
        ctx: &DeliverMintAuthorizationCtx,
        outcome: &str,
    ) -> Result<(), DeliverMintAuthorizationError> {
        error!(
            target: "tokenization",
            issuer_request_id = %self.issuer_request_id,
            outcome,
            "Mint authorization delivery parked; operator action required"
        );
        notify_swallowing_failure(
            ctx,
            &format!(
                "Mint authorization delivery for {} parked: {outcome}. The mint \
                 stays in MintAccepted until resolved on the issuance side.",
                self.issuer_request_id
            ),
        )
        .await;

        Ok(())
    }
}

/// Whether a live delivery row already exists for `issuer_request_id`. The
/// saga consults this before enqueueing: redrive successors are pushed via
/// `push_with_delay` and carry no idempotency key, so `push_idempotent`
/// alone cannot see them -- without this bound, a restart during a
/// delayed-redrive window would start a second delivery chain. Chains
/// deliver byte-identically (issuance-idempotent), so a duplicate is
/// benign rather than dangerous; this mirrors the one-poll-job-per-order
/// bound on `PollOrderStatus`. "Live" matches `requeue_orphaned`'s
/// in-flight definition: `Pending`, `Queued` (claimed, not yet running),
/// or `Running`.
pub(crate) async fn has_live_delivery_job(
    queue: &DeliverMintAuthorizationJobQueue,
    issuer_request_id: &IssuerRequestId,
) -> Result<bool, sqlx_apalis::Error> {
    let live_rows: i64 = sqlx_apalis::query_scalar(
        "SELECT COUNT(*) FROM Jobs \
         WHERE job_type = ? \
           AND json_valid(CAST(job AS TEXT)) \
           AND json_extract(CAST(job AS TEXT), '$.issuer_request_id') = ? \
           AND status IN ('Pending', 'Queued', 'Running')",
    )
    .bind(std::any::type_name::<DeliverMintAuthorization>())
    .bind(issuer_request_id.to_string())
    .fetch_one(queue.pool())
    .await?;

    Ok(live_rows > 0)
}

/// The delivery-relevant projection of the aggregate: `Some` only when a
/// signed authorization is waiting for issuance's acknowledgement.
fn awaiting_delivery(
    mint: &TokenizedEquityMint,
) -> Option<(TokenizationRequestId, SignedMintAuthorization)> {
    match mint {
        TokenizedEquityMint::MintAccepted {
            authorization: MintAuthorizationProgress::Signed(signed),
            tokenization_request_id,
            ..
        } => Some((tokenization_request_id.clone(), signed.clone())),
        // Delivered/NotSigned, and every state past or outside acceptance:
        // nothing awaits delivery. Terminal races (mint advanced or failed
        // while this job was queued) land here and no-op.
        TokenizedEquityMint::MintAccepted { .. }
        | TokenizedEquityMint::MintRequested { .. }
        | TokenizedEquityMint::TokensReceived { .. }
        | TokenizedEquityMint::WrapSubmitted { .. }
        | TokenizedEquityMint::TokensWrapped { .. }
        | TokenizedEquityMint::VaultDepositSubmitted { .. }
        | TokenizedEquityMint::DepositedIntoRaindex { .. }
        | TokenizedEquityMint::Failed { .. }
        | TokenizedEquityMint::Reconciled { .. } => None,
    }
}

/// Alert delivery failures are logged, never allowed to mask the job
/// outcome (mirrors the USDC transfer jobs' alert handling).
async fn notify_swallowing_failure(ctx: &DeliverMintAuthorizationCtx, message: &str) {
    if let Err(alert_error) = ctx.notifier.notify(message).await {
        warn!(
            target: "tokenization",
            ?alert_error,
            "Failed to deliver mint-authorization operator alert"
        );
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::Address;
    use httpmock::MockServer;
    use st0x_event_sorcery::test_store;
    use st0x_execution::Symbol;
    use st0x_float_macro::float;
    use st0x_raindex::Raindex;
    use st0x_tokenization::issuer_request_id;
    use st0x_tokenization::mock::MockTokenizer;
    use st0x_wrapper::{MockWrapper, Wrapper};

    use super::*;
    use crate::alerts::CapturingNotifier;
    use crate::bot_gas::BotGasReceiptCostEnqueuer;
    use crate::conductor::job::Job;
    use crate::mint_authorization::{
        ConfiguredMintAuthorizer, MockMintAuthorizer, StubMintAuthorizationDeliverer,
    };
    use crate::onchain::mock::MockRaindex;
    use crate::rebalancing::equity::EquityTransferServices;
    use crate::vault_lookup::MockVaultLookup;

    async fn build_ctx_with_deliverer(
        deliverer: Arc<dyn MintAuthorizationDeliverer>,
    ) -> (
        DeliverMintAuthorizationCtx,
        Arc<Store<TokenizedEquityMint>>,
        Arc<CapturingNotifier>,
    ) {
        let (pool, apalis_pool) = crate::test_utils::setup_test_pools().await;
        let raindex: Arc<dyn Raindex> = Arc::new(MockRaindex::new());
        let wrapper: Arc<dyn Wrapper> = Arc::new(MockWrapper::new());

        let services = EquityTransferServices {
            raindex,
            vault_lookup: Arc::new(MockVaultLookup::new()),
            tokenizer: Arc::new(MockTokenizer::new()),
            wrapper,
            bot_gas_enqueuer: BotGasReceiptCostEnqueuer::Disabled,
            mint_authorizer: ConfiguredMintAuthorizer::Enabled(Arc::new(MockMintAuthorizer)),
        };
        let mint_store = Arc::new(test_store(pool, services));
        let notifier = Arc::new(CapturingNotifier::default());

        let ctx = DeliverMintAuthorizationCtx {
            deliverer,
            mint_store: mint_store.clone(),
            notifier: notifier.clone(),
            job_queue: DeliverMintAuthorizationJobQueue::new(&apalis_pool),
        };
        (ctx, mint_store, notifier)
    }

    async fn build_ctx(
        outcome: MintAuthorizationDelivery,
    ) -> (
        DeliverMintAuthorizationCtx,
        Arc<Store<TokenizedEquityMint>>,
        Arc<CapturingNotifier>,
    ) {
        build_ctx_with_deliverer(Arc::new(StubMintAuthorizationDeliverer(outcome))).await
    }

    /// Drives a mint to `MintAccepted` with a `Signed` authorization -- the
    /// state the delivery job is enqueued from.
    async fn seed_signed_mint(store: &Store<TokenizedEquityMint>) -> IssuerRequestId {
        let id = issuer_request_id("ISS-DELIVERY");
        store
            .send(
                &id,
                TokenizedEquityMintCommand::RequestMint {
                    issuer_request_id: id.clone(),
                    symbol: Symbol::new("RKLB").unwrap(),
                    quantity: float!(10),
                    wallet: Address::ZERO,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                TokenizedEquityMintCommand::SubmitMintRequest {
                    issuer_request_id: id.clone(),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                TokenizedEquityMintCommand::SignMintAuthorization {
                    token: Address::repeat_byte(0x11),
                },
            )
            .await
            .unwrap();
        id
    }

    fn delivery_job(id: &IssuerRequestId) -> DeliverMintAuthorization {
        DeliverMintAuthorization {
            issuer_request_id: id.clone(),
            redrive_attempts: 0,
        }
    }

    async fn pending_delivery_rows(ctx: &DeliverMintAuthorizationCtx) -> i64 {
        sqlx_apalis::query_scalar::<_, i64>("SELECT COUNT(*) FROM Jobs WHERE job_type = ?")
            .bind(std::any::type_name::<DeliverMintAuthorization>())
            .fetch_one(ctx.job_queue.pool())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn recorded_outcome_marks_the_aggregate_delivered() {
        let (ctx, mint_store, notifier) = build_ctx(MintAuthorizationDelivery::Recorded).await;
        let id = seed_signed_mint(&mint_store).await;

        delivery_job(&id).perform(&ctx).await.unwrap();

        let entity = mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(
                entity,
                TokenizedEquityMint::MintAccepted {
                    authorization: MintAuthorizationProgress::Delivered(_),
                    ..
                }
            ),
            "expected Delivered after a 200, got: {entity:?}"
        );
        assert_eq!(notifier.messages().len(), 0);
    }

    /// 409/422 park the mint: one operator alert, `Ok` so the best-effort
    /// worker keeps running, aggregate untouched (still `Signed`).
    #[tokio::test]
    async fn rejected_outcome_parks_with_a_single_operator_alert() {
        let (ctx, mint_store, notifier) = build_ctx(MintAuthorizationDelivery::Rejected).await;
        let id = seed_signed_mint(&mint_store).await;

        delivery_job(&id).perform(&ctx).await.unwrap();

        let messages = notifier.messages();
        assert_eq!(messages.len(), 1, "exactly one alert: {messages:?}");
        assert!(
            messages[0].contains("parked"),
            "alert must say the delivery parked: {}",
            messages[0]
        );
        let entity = mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(
                entity,
                TokenizedEquityMint::MintAccepted {
                    authorization: MintAuthorizationProgress::Signed(_),
                    ..
                }
            ),
            "a parked delivery must leave the authorization Signed, got: {entity:?}"
        );
    }

    /// An out-of-contract 4xx (defective request or credentials) parks
    /// like 409/422: redelivering identical bytes cannot succeed.
    #[tokio::test]
    async fn permanent_client_failure_parks_with_alert() {
        let (ctx, mint_store, notifier) =
            build_ctx(MintAuthorizationDelivery::PermanentFailure(401)).await;
        let id = seed_signed_mint(&mint_store).await;

        delivery_job(&id).perform(&ctx).await.unwrap();

        let messages = notifier.messages();
        assert_eq!(messages.len(), 1, "exactly one alert: {messages:?}");
        assert!(
            messages[0].contains("401"),
            "the alert must carry the offending status: {}",
            messages[0]
        );
        assert_eq!(
            pending_delivery_rows(&ctx).await,
            0,
            "no successor may follow a permanent failure"
        );
    }

    /// The initiation race (404) re-enqueues a delayed successor rather
    /// than erroring into the 3-attempt apalis budget.
    #[tokio::test]
    async fn mint_not_found_yet_schedules_a_delayed_redrive() {
        let (ctx, mint_store, notifier) =
            build_ctx(MintAuthorizationDelivery::MintNotFoundYet).await;
        let id = seed_signed_mint(&mint_store).await;

        delivery_job(&id).perform(&ctx).await.unwrap();

        assert_eq!(
            pending_delivery_rows(&ctx).await,
            1,
            "a delayed successor must be enqueued"
        );
        assert_eq!(notifier.messages().len(), 0);
    }

    /// Past the fast budget the chain alerts once and degrades to the slow
    /// cadence instead of dead-lettering: the initial enqueue's idempotency
    /// key survives in the finished row and blocks a replacement chain, so
    /// a dead chain would strand the mint. The slow successor keeps the
    /// chain alive to complete once issuance recovers, and later slow
    /// redrives must not repeat the alert.
    #[tokio::test]
    async fn exhausted_fast_budget_alerts_once_and_degrades_to_slow_redrives() {
        let (ctx, mint_store, notifier) =
            build_ctx(MintAuthorizationDelivery::MintNotFoundYet).await;
        let id = seed_signed_mint(&mint_store).await;

        DeliverMintAuthorization {
            issuer_request_id: id.clone(),
            redrive_attempts: MAX_DELIVERY_REDRIVES,
        }
        .perform(&ctx)
        .await
        .unwrap();

        let messages = notifier.messages();
        assert_eq!(
            messages.len(),
            1,
            "exactly one boundary alert: {messages:?}"
        );
        assert!(
            messages[0].contains("keeps redelivering"),
            "the alert must say the chain stays alive: {}",
            messages[0]
        );
        let slow_successor_run_at: i64 = sqlx_apalis::query_scalar(
            "SELECT run_at - strftime('%s', 'now') FROM Jobs WHERE job_type = ?",
        )
        .bind(std::any::type_name::<DeliverMintAuthorization>())
        .fetch_one(ctx.job_queue.pool())
        .await
        .unwrap();
        let slow_delay_seconds: i64 = DELIVERY_SLOW_REDRIVE_DELAY.as_secs().try_into().unwrap();
        assert!(
            slow_successor_run_at > slow_delay_seconds - 60,
            "the successor must be scheduled at the slow cadence, \
             got {slow_successor_run_at}s out"
        );

        DeliverMintAuthorization {
            issuer_request_id: id,
            redrive_attempts: MAX_DELIVERY_REDRIVES + 5,
        }
        .perform(&ctx)
        .await
        .unwrap();

        assert_eq!(
            notifier.messages().len(),
            1,
            "slow redrives past the boundary must not re-alert"
        );
        assert_eq!(
            pending_delivery_rows(&ctx).await,
            2,
            "every slow redrive must keep the chain alive"
        );
    }

    /// The wire-level half of nonce fixity, over the real HTTP client: the
    /// initiation race (404, issuance has no mint yet) followed by a retry
    /// must put the SAME persisted nonce and signature on the wire, byte
    /// for byte -- issuance treats an identical redelivery as an idempotent
    /// `200`, while a differing one is a `409` conflict. Exact-JSON-body
    /// matchers on both mocks prove each attempt carried exactly the
    /// persisted authorization.
    #[tokio::test]
    async fn redelivery_after_initiation_race_is_byte_identical_on_the_wire() {
        use httpmock::Method::POST;

        let server = MockServer::start_async().await;
        let client = st0x_issuance_client::IssuanceClient::new(
            url::Url::parse(&server.base_url()).expect("valid mock URL"),
            "test-key",
        )
        .expect("client builds");
        let (ctx, mint_store, notifier) = build_ctx_with_deliverer(Arc::new(client)).await;
        let id = seed_signed_mint(&mint_store).await;

        let TokenizedEquityMint::MintAccepted {
            authorization: MintAuthorizationProgress::Signed(signed),
            tokenization_request_id,
            ..
        } = mint_store.load(&id).await.unwrap().unwrap()
        else {
            panic!("expected a Signed MintAccepted mint after seeding");
        };
        let path = format!(
            "/internal/mints/{}/authorization",
            tokenization_request_id.as_ref()
        );
        let expected_body = serde_json::json!({
            "nonce": signed.nonce.to_string(),
            "signature": signed.signature.to_string(),
        });

        let not_found_yet = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path(&path)
                    .json_body(expected_body.clone());
                then.status(404);
            })
            .await;
        delivery_job(&id).perform(&ctx).await.unwrap();
        not_found_yet.assert_async().await;
        not_found_yet.delete_async().await;

        let recorded = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path(&path)
                    .json_body(expected_body.clone());
                then.status(200).json_body(serde_json::json!({
                    "issuer_request_id": "550e8400-e29b-41d4-a716-446655440000",
                    "status": "authorized",
                }));
            })
            .await;
        DeliverMintAuthorization {
            issuer_request_id: id.clone(),
            redrive_attempts: 1,
        }
        .perform(&ctx)
        .await
        .unwrap();
        recorded.assert_async().await;

        let entity = mint_store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(
                entity,
                TokenizedEquityMint::MintAccepted {
                    authorization: MintAuthorizationProgress::Delivered(_),
                    ..
                }
            ),
            "the 200 retry must record the delivery, got: {entity:?}"
        );
        assert_eq!(notifier.messages().len(), 0);
    }

    /// A mint that already recorded its delivery (or advanced past
    /// acceptance) makes the job a no-op -- no delivery attempt, no alert.
    #[tokio::test]
    async fn already_delivered_mint_is_a_noop() {
        // A Conflict outcome would park+alert IF the deliverer were called;
        // an empty notifier therefore proves the early return.
        let (ctx, mint_store, notifier) = build_ctx(MintAuthorizationDelivery::Conflict).await;
        let id = seed_signed_mint(&mint_store).await;
        mint_store
            .send(
                &id,
                TokenizedEquityMintCommand::RecordMintAuthorizationDelivered,
            )
            .await
            .unwrap();

        delivery_job(&id).perform(&ctx).await.unwrap();

        assert_eq!(notifier.messages().len(), 0);
    }
}

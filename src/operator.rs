//! Narrow application boundary used by the operator CLI.
//!
//! The CLI is a separate application crate. This module exposes only the
//! domain operations and types that application needs while keeping the
//! implementation modules themselves private.

use chrono::{DateTime, NaiveDate, Utc};
use st0x_evm::Chain;
use st0x_execution::{ExecutorOrderId, FractionalShares, Symbol};

use crate::offchain::order::OffchainOrderId;
use crate::onchain_trade::OnChainTradeId;

/// A caller-facing reason an operator recovery command refused to apply, each
/// variant carrying the typed context its message renders.
#[derive(Debug, thiserror::Error)]
pub enum RejectionReason {
    #[error("--reason must not be blank; it is persisted as the audit record")]
    BlankReason,
    #[error("--source must not be blank; it is persisted as audit provenance")]
    BlankSource,
    #[error(
        "--observed-at must identify the regular-session close before the {day} 00:05 ET \
         capture boundary ({boundary})"
    )]
    ObservedAtAfterCaptureBoundary {
        day: NaiveDate,
        boundary: DateTime<Utc>,
    },
    #[error(
        "{symbol} has no [chains.<name>.trading.assets.equities] entry, so its {unconverted} \
         wrapped-location row(s) on {day} hold vault shares, not underlying shares. A mark \
         would price them as underlying and misstate the day's capital. Reconcile the holding \
         instead, or restore the config entry so the capture can resolve a vault ratio."
    )]
    UnconvertedWrappedEquityRows {
        symbol: Symbol,
        unconverted: i64,
        day: NaiveDate,
    },
    #[error(
        "position {symbol} has pending offchain order {pending}; run position release-hedge \
         before setting position"
    )]
    PositionHasPendingOrder {
        symbol: Symbol,
        pending: OffchainOrderId,
    },
    #[error("position {symbol} not found")]
    PositionNotFound { symbol: Symbol },
    #[error(
        "OffchainOrder {offchain_order_id} belongs to {owner}, not {symbol} -- refusing to \
         repair"
    )]
    OffchainOrderBelongsToOtherSymbol {
        offchain_order_id: OffchainOrderId,
        owner: Symbol,
        symbol: Symbol,
    },
    #[error(
        "OffchainOrder {offchain_order_id} is PartiallyFilled: shares already executed \
         offchain, and failing it would erase that hedge from the position. Reconcile the \
         partial fill first."
    )]
    OffchainOrderPartiallyFilled { offchain_order_id: OffchainOrderId },
    #[error(
        "OffchainOrder {offchain_order_id} is Filled: the hedge executed. This command cannot \
         repair a filled order -- reconcile the fill into the position instead of failing it."
    )]
    OffchainOrderFilled { offchain_order_id: OffchainOrderId },
    #[error(
        "OffchainOrder {offchain_order_id} is in a cancellation lifecycle state: this command \
         fails stuck Pending/Submitted orders, not cancellations -- refusing. Confirm the \
         intended recovery path for cancellation states."
    )]
    OffchainOrderInCancellationLifecycle { offchain_order_id: OffchainOrderId },
    #[error("position {symbol} pending offchain order is {pending}, not {offchain_order_id}")]
    PendingPointerMismatch {
        symbol: Symbol,
        pending: OffchainOrderId,
        offchain_order_id: OffchainOrderId,
    },
    #[error(
        "position {symbol} has no pending offchain order and no OffchainOrder aggregate \
         {offchain_order_id} exists -- nothing to repair"
    )]
    NothingToRepair {
        symbol: Symbol,
        offchain_order_id: OffchainOrderId,
    },
    #[error(
        "OffchainOrder {offchain_order_id} has executed shares -- refusing to erase the \
         executed hedge"
    )]
    OffchainOrderHasExecutedShares { offchain_order_id: OffchainOrderId },
    #[error(
        "OffchainOrder {offchain_order_id} acquired executed shares concurrently; refusing to \
         erase the executed hedge -- reconcile the fill into the position."
    )]
    AcquiredExecutedSharesConcurrently { offchain_order_id: OffchainOrderId },
    #[error(
        "OffchainOrder {offchain_order_id} changed concurrently while it was being failed; \
         it still holds no executed shares -- re-run the release."
    )]
    OffchainOrderChangedConcurrently { offchain_order_id: OffchainOrderId },
    #[error("Fill {trade_id}: missing block_number, cannot witness fill")]
    FillMissingBlockNumber { trade_id: OnChainTradeId },
    #[error(
        "offchain order {offchain_order_id} for {symbol} is in an unexpected post-placement \
         state; refusing to clear the position claim"
    )]
    OffchainOrderUnexpectedPostPlacementState {
        offchain_order_id: OffchainOrderId,
        symbol: Symbol,
    },
    #[error(
        "offchain order {offchain_order_id} for {symbol} has {shares_filled} filled shares \
         without an average price; refusing to clear the position claim"
    )]
    OffchainOrderUnpricedFill {
        offchain_order_id: OffchainOrderId,
        symbol: Symbol,
        shares_filled: FractionalShares,
    },
    #[error(
        "broker order {executor_order_id} already exists for failed anchor {anchor}; let the \
         liquidity service reconcile it before processing this fill"
    )]
    FailedAnchorStillAtBroker {
        anchor: OffchainOrderId,
        executor_order_id: ExecutorOrderId,
    },
    #[error(
        "process-tx decoded a fill on {decoded}, but the request selected {requested}; refusing \
         to hedge a fill from a chain other than the one requested"
    )]
    DecodedChainMismatch { requested: Chain, decoded: Chain },
    #[error(
        "position {symbol} holds pending offchain order {offchain_order_id} that was never sent \
         to the broker, and the trading schedule is disabled so it is not a deferred retry; \
         refusing to place over it and preserving the claim for reconciliation"
    )]
    RetainedPendingWithoutSchedule {
        offchain_order_id: OffchainOrderId,
        symbol: Symbol,
    },
}

/// The failure of a shared operator recovery command, letting a caller-facing
/// rejection and an operational failure map to different results.
#[derive(Debug, thiserror::Error)]
pub enum OperatorError {
    /// The request cannot be applied in the aggregate's current state, or an
    /// input was invalid; the caller surfaces this to the operator.
    #[error(transparent)]
    Rejected(#[from] RejectionReason),
    /// A placement preflight returned a reservation for the wrong direction or
    /// symbol; an internal invariant violation surfaced to the caller as a 500
    /// whose body carries this typed reason.
    #[error(transparent)]
    PreflightReservationMismatch(#[from] process_tx::PreflightReservationMismatch),
    /// An infrastructure failure while loading or sending a command.
    #[error("{0:#}")]
    Operational(anyhow::Error),
}

impl From<anyhow::Error> for OperatorError {
    fn from(error: anyhow::Error) -> Self {
        Self::Operational(error)
    }
}

pub mod api {
    pub use crate::api::ResumeResponse;
}

pub mod bot_gas {
    pub use crate::bot_gas::BotGasReceiptCostEnqueuer;
}

pub mod conductor {
    pub use crate::conductor::{
        FillAccountingOutcome, account_for_onchain_fill, configured_equity_symbols,
        execute_mark_acknowledged, execute_settle_fill, is_expected_place_offchain_order_rejection,
    };

    #[cfg(feature = "test-support")]
    pub use crate::conductor::{
        TradeProcessingCqrs, execute_acknowledge_fill, process_queued_trade,
    };

    pub mod job {
        pub use crate::conductor::job::{
            BackpressureStreak, QueuePushError, decide_backpressure, find_backpressure,
        };
    }
}

pub mod equity_redemption {
    pub use crate::equity_redemption::{
        EquityRedemption, EquityRedemptionCommand, RedemptionAggregateId,
    };

    #[cfg(feature = "test-support")]
    pub use crate::equity_redemption::{
        DetectionFailure, EquityRedemptionError, redemption_aggregate_id,
    };
}

/// Recovery operations shared by the operator CLI and end-to-end tests.
pub mod equity_transfer {
    use std::time::Duration;

    use st0x_config::Ctx;
    use st0x_event_sorcery::{SendError, Store};
    use st0x_tokenization::IssuerRequestId;

    use crate::equity_redemption::{
        DetectionFailure, EquityRedemption, EquityRedemptionCommand, RedemptionAggregateId,
    };
    use crate::tokenized_equity_mint::{TokenizedEquityMint, TokenizedEquityMintCommand};

    const OPERATOR_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

    /// The equity-transfer aggregate targeted by an operator recovery.
    #[derive(Debug, Clone, Copy)]
    pub enum EquityTransferKind {
        Mint,
        Redemption,
    }

    /// The transfer kind addressed by an operator recheck.
    ///
    /// A superset of [`EquityTransferKind`]: a failed BaseToAlpaca USDC
    /// deposit is recheckable against Alpaca, while `fail` and the
    /// aggregate-level recovery commands stay equity-only (a stuck USDC
    /// transfer is reconciled, never force-failed).
    #[derive(Debug, Clone, Copy)]
    pub enum RecheckKind {
        Mint,
        Redemption,
        Usdc,
    }

    /// A force-failure request with no auditable explanation.
    #[derive(Debug, thiserror::Error)]
    #[error("--reason must not be blank; it is persisted as the audit record")]
    pub(crate) struct InvalidFailureReason;

    /// Errors returned while force-failing an equity transfer in the running
    /// bot.
    #[derive(Debug, thiserror::Error)]
    pub(crate) enum FailTransferError {
        #[error(transparent)]
        InvalidReason(#[from] InvalidFailureReason),
        #[error("invalid mint id")]
        InvalidMintId(#[source] uuid::Error),
        #[error("invalid redemption id")]
        InvalidRedemptionId(#[source] uuid::Error),
        #[error("mint aggregate not found: {0}")]
        MintNotFound(IssuerRequestId),
        #[error("redemption aggregate not found: {0}")]
        RedemptionNotFound(RedemptionAggregateId),
        #[error("mint {0} already completed")]
        MintAlreadyCompleted(IssuerRequestId),
        #[error("mint {0} already failed")]
        MintAlreadyFailed(IssuerRequestId),
        #[error("mint {0} already reconciled")]
        MintAlreadyReconciled(IssuerRequestId),
        #[error("redemption {0} already completed")]
        RedemptionAlreadyCompleted(RedemptionAggregateId),
        #[error("redemption {0} already failed")]
        RedemptionAlreadyFailed(RedemptionAggregateId),
        #[error("redemption {0} already reconciled")]
        RedemptionAlreadyReconciled(RedemptionAggregateId),
        #[error("mint store operation failed")]
        MintStore(#[source] Box<SendError<TokenizedEquityMint>>),
        #[error("redemption store operation failed")]
        RedemptionStore(#[source] Box<SendError<EquityRedemption>>),
    }

    #[cfg(any(test, feature = "test-support"))]
    fn stale_state_context<Failure: std::error::Error + Send + Sync + 'static>(
        kind: &str,
        id: &str,
        error: st0x_event_sorcery::AggregateError<Failure>,
    ) -> anyhow::Error {
        match error {
            rejection @ (st0x_event_sorcery::AggregateError::UserError(_)
            | st0x_event_sorcery::AggregateError::AggregateConflict) => {
                anyhow::Error::new(rejection).context(format!(
                    "{kind} {id} rejected the failure command. The state may have \
                 advanced since it was read (is the bot driving this aggregate \
                 concurrently?) -- re-run to see the current state."
                ))
            }
            infrastructure @ (st0x_event_sorcery::AggregateError::DatabaseConnectionError(_)
            | st0x_event_sorcery::AggregateError::DeserializationError(_)
            | st0x_event_sorcery::AggregateError::UnexpectedError(_)) => {
                anyhow::Error::new(infrastructure)
            }
        }
    }

    /// Exposes stale-state error mapping to application-boundary tests.
    #[cfg(feature = "test-support")]
    pub fn stale_state_context_for_test<Failure: std::error::Error + Send + Sync + 'static>(
        kind: &str,
        id: &str,
        error: st0x_event_sorcery::AggregateError<Failure>,
    ) -> anyhow::Error {
        stale_state_context(kind, id, error)
    }

    pub(crate) fn validate_failure_reason(reason: &str) -> Result<(), InvalidFailureReason> {
        if reason.trim().is_empty() {
            return Err(InvalidFailureReason);
        }

        Ok(())
    }

    fn mint_failure_command(
        entity: &TokenizedEquityMint,
        id: &IssuerRequestId,
        reason: &str,
    ) -> Result<TokenizedEquityMintCommand, FailTransferError> {
        match entity {
            TokenizedEquityMint::MintRequested { .. }
            | TokenizedEquityMint::MintAccepted { .. } => {
                Ok(TokenizedEquityMintCommand::FailAcceptance {
                    reason: reason.to_string(),
                })
            }
            TokenizedEquityMint::TokensReceived { .. }
            | TokenizedEquityMint::WrapSubmitted { .. } => {
                Ok(TokenizedEquityMintCommand::FailWrapping {
                    reason: reason.to_string(),
                })
            }
            TokenizedEquityMint::TokensWrapped { .. }
            | TokenizedEquityMint::VaultDepositSubmitted { .. } => {
                Ok(TokenizedEquityMintCommand::FailRaindexDeposit {
                    reason: reason.to_string(),
                })
            }
            TokenizedEquityMint::DepositedIntoRaindex { .. } => {
                Err(FailTransferError::MintAlreadyCompleted(id.clone()))
            }
            TokenizedEquityMint::Failed { .. } => {
                Err(FailTransferError::MintAlreadyFailed(id.clone()))
            }
            TokenizedEquityMint::Reconciled { .. } => {
                Err(FailTransferError::MintAlreadyReconciled(id.clone()))
            }
        }
    }

    fn redemption_failure_command(
        entity: &EquityRedemption,
        id: &RedemptionAggregateId,
        reason: &str,
    ) -> Result<EquityRedemptionCommand, FailTransferError> {
        match entity {
            EquityRedemption::VaultWithdrawPending { .. }
            | EquityRedemption::VaultWithdrawSubmitted { .. }
            | EquityRedemption::WithdrawnFromRaindex { .. }
            | EquityRedemption::UnwrapPending { .. }
            | EquityRedemption::UnwrapSubmitted { .. }
            | EquityRedemption::TokensUnwrapped { .. }
            | EquityRedemption::SendPending { .. } => Ok(EquityRedemptionCommand::FailTransfer {
                reason: reason.to_string(),
            }),
            EquityRedemption::TokensSent { .. } => Ok(EquityRedemptionCommand::FailDetection {
                failure: DetectionFailure::Operator {
                    reason: reason.to_string(),
                },
            }),
            EquityRedemption::Pending { .. } => Ok(EquityRedemptionCommand::RejectRedemption {
                reason: reason.to_string(),
            }),
            EquityRedemption::Completed { .. } => {
                Err(FailTransferError::RedemptionAlreadyCompleted(id.clone()))
            }
            EquityRedemption::Failed { .. } => {
                Err(FailTransferError::RedemptionAlreadyFailed(id.clone()))
            }
            EquityRedemption::Reconciled { .. } => {
                Err(FailTransferError::RedemptionAlreadyReconciled(id.clone()))
            }
        }
    }

    /// Marks a stuck mint or redemption aggregate as failed through the
    /// conductor-owned stores, ensuring the live reactors observe the event.
    pub(crate) async fn fail_transfer_in_process(
        mint_store: &Store<TokenizedEquityMint>,
        redemption_store: &Store<EquityRedemption>,
        transfer_kind: EquityTransferKind,
        id: &str,
        reason: &str,
    ) -> Result<(), FailTransferError> {
        validate_failure_reason(reason)?;

        match transfer_kind {
            EquityTransferKind::Mint => {
                let mint_id: IssuerRequestId =
                    id.parse().map_err(FailTransferError::InvalidMintId)?;
                let entity = mint_store
                    .load(&mint_id)
                    .await
                    .map_err(|error| FailTransferError::MintStore(Box::new(error)))?
                    .ok_or_else(|| FailTransferError::MintNotFound(mint_id.clone()))?;
                let command = mint_failure_command(&entity, &mint_id, reason)?;

                mint_store
                    .send(&mint_id, command)
                    .await
                    .map_err(|error| FailTransferError::MintStore(Box::new(error)))?;
            }
            EquityTransferKind::Redemption => {
                let redemption_id: RedemptionAggregateId =
                    id.parse().map_err(FailTransferError::InvalidRedemptionId)?;
                let entity = redemption_store
                    .load(&redemption_id)
                    .await
                    .map_err(|error| FailTransferError::RedemptionStore(Box::new(error)))?
                    .ok_or_else(|| FailTransferError::RedemptionNotFound(redemption_id.clone()))?;
                let command = redemption_failure_command(&entity, &redemption_id, reason)?;

                redemption_store
                    .send(&redemption_id, command)
                    .await
                    .map_err(|error| FailTransferError::RedemptionStore(Box::new(error)))?;
            }
        }

        Ok(())
    }

    /// Test-fixture helper for persisting a forced failure before the server
    /// starts. Production operator commands must use [`fail_transfer`] so the
    /// running bot observes the event.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn fail_transfer_in_database(
        pool: &sqlx::SqlitePool,
        transfer_kind: EquityTransferKind,
        id: &str,
        reason: &str,
    ) -> anyhow::Result<()> {
        validate_failure_reason(reason)?;

        let services = crate::rebalancing::equity::EquityTransferServices::panicking();
        match transfer_kind {
            EquityTransferKind::Mint => {
                let mint_id: IssuerRequestId = id
                    .parse()
                    .map_err(|error| anyhow::anyhow!("Invalid mint id {id:?}: {error}"))?;
                let entity = st0x_event_sorcery::load_entity::<TokenizedEquityMint>(pool, &mint_id)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("Mint aggregate not found: {id}"))?;
                let command = mint_failure_command(&entity, &mint_id, reason)?;

                st0x_event_sorcery::send_command::<TokenizedEquityMint>(
                    pool, &mint_id, command, services,
                )
                .await
                .map_err(|error| stale_state_context("Mint", id, error))?;
            }
            EquityTransferKind::Redemption => {
                let redemption_id: RedemptionAggregateId = id
                    .parse()
                    .map_err(|error| anyhow::anyhow!("Invalid redemption ID: {error}"))?;
                let entity =
                    st0x_event_sorcery::load_entity::<EquityRedemption>(pool, &redemption_id)
                        .await?
                        .ok_or_else(|| anyhow::anyhow!("Redemption aggregate not found: {id}"))?;
                let command = redemption_failure_command(&entity, &redemption_id, reason)?;

                st0x_event_sorcery::send_command::<EquityRedemption>(
                    pool,
                    &redemption_id,
                    command,
                    services,
                )
                .await
                .map_err(|error| stale_state_context("Redemption", id, error))?;
            }
        }

        Ok(())
    }

    /// Returns the loopback server endpoint used to force-fail a transfer.
    pub fn fail_url(ctx: &Ctx, transfer_kind: EquityTransferKind, id: &str) -> String {
        let kind = match transfer_kind {
            EquityTransferKind::Mint => "equity_mint",
            EquityTransferKind::Redemption => "equity_redemption",
        };

        format!(
            "http://127.0.0.1:{}/transfers/fail/{kind}/{id}",
            ctx.server_port
        )
    }

    async fn post_operator_request(
        url: &str,
        operation: &str,
        body: Option<&serde_json::Value>,
    ) -> anyhow::Result<String> {
        let client = reqwest::Client::builder()
            .timeout(OPERATOR_REQUEST_TIMEOUT)
            .build()?;
        let request = client.post(url);
        let response = match body {
            Some(body) => request.json(body),
            None => request,
        }
        .send()
        .await
        .map_err(|error| {
            if error.is_connect() {
                anyhow::Error::new(error)
                    .context(format!("could not reach the bot at {url}; is it running?"))
            } else {
                anyhow::Error::new(error)
            }
        })?;
        let status = response.status();
        let response_body = response.text().await?;

        if !status.is_success() {
            anyhow::bail!("{operation} failed ({status}): {response_body}");
        }

        Ok(response_body)
    }

    /// Requests an operator-forced failure from the running bot.
    pub async fn fail_transfer(
        ctx: &Ctx,
        transfer_kind: EquityTransferKind,
        id: &str,
        reason: &str,
    ) -> anyhow::Result<()> {
        validate_failure_reason(reason)?;

        let url = fail_url(ctx, transfer_kind, id);
        let body = serde_json::json!({ "reason": reason });
        post_operator_request(&url, "transfer fail", Some(&body)).await?;

        Ok(())
    }

    /// Returns the local server endpoint used to re-check a transfer.
    pub fn recheck_url(ctx: &Ctx, transfer_kind: RecheckKind, id: &str) -> String {
        let kind = match transfer_kind {
            RecheckKind::Mint => "equity_mint",
            RecheckKind::Redemption => "equity_redemption",
            RecheckKind::Usdc => "usdc_bridge",
        };

        format!(
            "http://127.0.0.1:{}/transfers/recheck/{kind}/{id}",
            ctx.server_port
        )
    }

    /// Requests an in-process re-check and returns its operator-facing outcome.
    pub async fn recheck_transfer(
        ctx: &Ctx,
        transfer_kind: RecheckKind,
        id: &str,
    ) -> anyhow::Result<String> {
        let url = recheck_url(ctx, transfer_kind, id);
        let body = post_operator_request(&url, "transfer recheck", None).await?;

        Ok(serde_json::from_str::<serde_json::Value>(&body)
            .ok()
            .and_then(|value| {
                value
                    .get("outcome")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            })
            .unwrap_or(body))
    }

    #[cfg(test)]
    mod tests {
        use super::validate_failure_reason;

        #[test]
        fn fail_transfer_rejects_blank_audit_reasons_before_dispatch() {
            for reason in ["", " ", "\t\n"] {
                let error = validate_failure_reason(reason).unwrap_err();

                assert_eq!(
                    error.to_string(),
                    "--reason must not be blank; it is persisted as the audit record"
                );
            }
        }
    }
}

pub mod inventory {
    pub use crate::inventory::{PortfolioAsset, PortfolioBalanceRow, PortfolioLocation};

    #[cfg(feature = "test-support")]
    pub use st0x_config::ImbalanceThreshold;
}

pub mod mint_authorization {
    pub use crate::mint_authorization::{ConfiguredMintAuthorizer, VaultModeReader};

    #[cfg(feature = "test-support")]
    pub use crate::mint_authorization::StubVaultModeReader;
}

pub mod native_gas {
    pub use crate::native_gas::{ConfiguredGasReadiness, GasReadiness};
}

pub mod offchain {
    pub mod order {
        pub use crate::offchain::order::{
            BrokerOrderPlacement, OffchainOrder, OffchainOrderCommand, OffchainOrderError,
            OffchainOrderId, OffchainOrderPlacement, OrderPlacementResult, OrderPlacer,
            TerminalPositionFinalization, client_order_id_for_placement,
            place_offchain_order_at_broker, position_command_for_finalization,
            terminal_position_finalization,
        };

        #[cfg(feature = "test-support")]
        pub use crate::offchain::order::{
            CancellationReason, CounterTradeOrderKind, OffchainOrderEvent,
            OffchainOrderFailureKind, PollOrderStatusJobQueue, noop_order_placer,
        };
    }

    pub use crate::trading::offchain::hedge::{
        acquire_counter_trade_submission_file_lock, live_buying_power_reservations,
    };
}

pub mod onchain {
    pub use crate::onchain::{OnChainError, OnchainTrade, TradeValidationError, raindex_contracts};

    pub mod accumulator {
        pub use crate::onchain::accumulator::check_execution_readiness;
    }

    pub mod trade {
        pub use crate::onchain::trade::{BotOperator, RecoveryActors};

        #[cfg(feature = "test-support")]
        pub use crate::onchain::trade::RaindexTradeEvent;
    }

    #[cfg(feature = "test-support")]
    pub mod mock {
        pub use crate::onchain::mock::MockRaindex;
    }
}

pub mod onchain_trade {
    pub use crate::onchain_trade::{OnChainTrade, OnChainTradeId};

    #[cfg(feature = "test-support")]
    pub use crate::onchain_trade::{InventoryVenue, OnChainTradeCommand, OnChainTradeSource};
}

pub mod performance {
    pub mod equity_timing {
        pub use crate::performance::equity_timing::EquityTimingProjection;
    }

    pub mod rebalance {
        pub use crate::performance::rebalance::RebalanceTimingProjection;
    }

    pub mod reliability {
        pub use crate::performance::reliability::LifecycleFailureProjection;
    }
}

pub mod portfolio_snapshot {
    use std::sync::Arc;

    use anyhow::Context;
    use chrono::{DateTime, NaiveDate, TimeZone, Utc};
    use chrono_tz::America::New_York;
    use rain_math_float::Float;
    use sqlx::SqlitePool;
    use st0x_config::Ctx;
    use st0x_event_sorcery::{RetryOnBusy, StoreBuilder, load_entity};
    use st0x_execution::Symbol;
    use st0x_finance::Positive;
    use st0x_float_serde::format_float;

    use crate::conductor::configured_equity_symbols;
    use crate::inventory::PortfolioLocation;
    use crate::operator::{OperatorError, RejectionReason};

    pub use crate::portfolio_snapshot::{
        PortfolioBalanceRowWithMark, PortfolioSnapshot, PortfolioSnapshotCommand,
        PortfolioSnapshotId, PortfolioSnapshotProjection,
    };

    /// The verified detail of a persisted historical mark, for the caller to
    /// render or serialize.
    #[derive(Debug, Clone)]
    pub struct SetEquityMarkOutcome {
        /// The stored USD mark, formatted exactly as it was persisted.
        pub formatted_mark: String,
    }

    /// The audited inputs for a historical portfolio mark correction, grouped so
    /// the `source` and `reason` strings cannot be swapped at a call site.
    #[derive(Debug, Clone)]
    pub struct EquityMarkCorrection {
        /// ET day of the captured balance snapshot.
        pub day: NaiveDate,
        /// Equity symbol whose mark applies at every captured location.
        pub symbol: Symbol,
        /// Strictly-positive historical USD closing price per share.
        pub usd_mark: Positive<Float>,
        /// Sourced economic timestamp; an earlier ET day.
        pub observed_at: DateTime<Utc>,
        /// Source used to verify the historical price.
        pub source: String,
        /// Operator reason persisted with the correction event.
        pub reason: String,
    }

    /// Sets the audited historical closing-price mark for one captured ET day.
    ///
    /// Shared by the operator CLI and the ops API. Rejects an `observed_at` at
    /// or after the day's 00:05 ET capture boundary, and refuses an
    /// unconfigured symbol that still holds unconverted wrapped-equity rows (a
    /// mark would price vault shares as underlying and misstate the day). On
    /// success the read model is verified to have updated every captured row.
    pub async fn set_equity_mark(
        pool: &SqlitePool,
        ctx: &Ctx,
        correction: &EquityMarkCorrection,
    ) -> Result<SetEquityMarkOutcome, OperatorError> {
        let &EquityMarkCorrection {
            day,
            ref symbol,
            usd_mark,
            observed_at,
            ref source,
            ref reason,
        } = correction;
        if source.trim().is_empty() {
            return Err(RejectionReason::BlankSource.into());
        }
        if reason.trim().is_empty() {
            return Err(RejectionReason::BlankReason.into());
        }

        let capture_boundary = New_York
            .from_local_datetime(
                &day.and_hms_opt(0, 5, 0)
                    .context("invalid ET capture time")?,
            )
            .single()
            .context("ambiguous ET capture boundary")?
            .with_timezone(&Utc);
        if observed_at >= capture_boundary {
            return Err(RejectionReason::ObservedAtAfterCaptureBoundary {
                day,
                boundary: capture_boundary,
            }
            .into());
        }

        // `EquityMarkSet` prices EVERY row of the symbol (the projection's UPDATE
        // has no location filter). A symbol with no `[chains.<name>.trading.assets.equities]` entry has
        // no wrapper entry either, so the capture leaves its MarketMaking and
        // BaseWalletWrapped rows in vault-share units -- applying an underlying
        // share price to those misvalues the day, which is exactly what the
        // capture's forced-absent mark prevents. Refuse rather than let one repair
        // reintroduce it. Rows at those locations exist only when nonzero: the
        // capture drops the empty ones.
        if !configured_equity_symbols(ctx).contains(symbol) {
            let market_making = PortfolioLocation::MarketMaking(ctx.chains.primary().chain);
            let unconverted: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM portfolio_snapshot \
                 WHERE et_day = ? AND asset = ? AND location IN (?, ?)",
            )
            .bind(day.to_string())
            .bind(symbol.to_string())
            .bind(market_making.to_string())
            .bind(PortfolioLocation::BaseWalletWrapped.to_string())
            .fetch_one(pool)
            .await
            .context("failed to check for unconverted wrapped-equity rows")?;

            if unconverted > 0 {
                return Err(RejectionReason::UnconvertedWrappedEquityRows {
                    symbol: symbol.clone(),
                    unconverted,
                    day,
                }
                .into());
            }
        }

        let store = StoreBuilder::<PortfolioSnapshot>::new(pool.clone())
            .with(Arc::new(RetryOnBusy {
                inner: PortfolioSnapshotProjection::new(pool.clone()),
            }))
            .build(())
            .await
            .context("failed to build portfolio snapshot store")?;

        store
            .send(
                &PortfolioSnapshotId(day),
                PortfolioSnapshotCommand::SetEquityMark {
                    symbol: symbol.clone(),
                    usd_mark,
                    observed_at,
                    source: source.clone(),
                    reason: reason.clone(),
                    corrected_at: Utc::now(),
                },
            )
            .await
            .context("failed to set historical portfolio snapshot mark")?;

        let formatted_mark =
            format_float(&usd_mark.inner()).context("failed to format USD mark")?;
        let snapshot = load_entity::<PortfolioSnapshot>(pool, &PortfolioSnapshotId(day))
            .await
            .context("failed to reload corrected portfolio snapshot")?
            .context("corrected portfolio snapshot aggregate is missing")?;
        let expected_row_count = i64::try_from(snapshot.captured_equity_row_count(symbol))
            .context("captured equity row count exceeds SQLite integer range")?;
        let (row_count, corrected_count): (i64, i64) = sqlx::query_as(
            "SELECT COUNT(*), COUNT(CASE WHEN usd_mark = ? AND mark_captured_at = ? THEN 1 END) \
             FROM portfolio_snapshot WHERE et_day = ? AND asset = ?",
        )
        .bind(&formatted_mark)
        .bind(observed_at.to_rfc3339())
        .bind(day.to_string())
        .bind(symbol.to_string())
        .fetch_one(pool)
        .await
        .context("failed to verify historical portfolio snapshot mark")?;
        if row_count != expected_row_count || corrected_count != expected_row_count {
            return Err(OperatorError::Operational(anyhow::anyhow!(
                "historical mark event committed, but the portfolio-snapshot read model did not \
                 update every {day} {symbol} row; run `view rebuild --aggregate \
                 portfolio-snapshot --all` before retrying"
            )));
        }

        Ok(SetEquityMarkOutcome { formatted_mark })
    }
}

pub mod position {
    use anyhow::Context;
    use rain_math_float::Float;
    use sqlx::SqlitePool;
    use st0x_config::ExecutionThreshold;
    use st0x_event_sorcery::{StoreBuilder, load_entity};
    use st0x_execution::{FractionalShares, Symbol};

    use crate::offchain::order::{OffchainOrder, OffchainOrderFailureKind, OffchainOrderId};
    use crate::operator::{OperatorError, RejectionReason};

    pub use crate::position::{
        AnchorDisposition, EquityTransferReservationId, Position, PositionCommand,
    };

    #[cfg(feature = "test-support")]
    pub use crate::position::{PositionEvent, TradeId};

    /// The net exposure change a completed [`set_position`] recorded.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct SetPositionOutcome {
        /// The position's net exposure before the adjustment.
        pub previous_net: FractionalShares,
    }

    /// Whether [`release_pending_offchain_order`] cleared the position's pending
    /// pointer or found it already clear from a prior partial run.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum PointerOutcome {
        /// This call cleared the pending pointer via `FailOffChainOrder`.
        ClearedNow,
        /// The pointer was already clear; only the orphaned aggregate was repaired.
        WasAlreadyClear,
    }

    /// What [`release_pending_offchain_order`] did with the orphaned
    /// `OffchainOrder` aggregate.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum OffchainOrderOutcome {
        /// Driven to its `Failed` terminal.
        MarkedFailed,
        /// Already `Failed`; left untouched.
        AlreadyTerminal,
        /// No aggregate existed for the pointer; nothing to fail.
        NoAggregate,
        /// Reached a terminal state concurrently; left untouched.
        TerminalConcurrently,
    }

    /// The result of releasing a position's pending offchain order pointer.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct ReleaseHedgeOutcome {
        /// What happened to the position's pending pointer.
        pub pointer: PointerOutcome,
        /// What happened to the orphaned `OffchainOrder` aggregate.
        pub offchain_order: OffchainOrderOutcome,
    }

    /// Sets a position's net exposure after an operator manual correction.
    ///
    /// Shared by the operator CLI and the ops API. Refuses while the position
    /// still holds a pending offchain order (release the hedge first), pins the
    /// send to the read `expected_net` so a concurrent change is rejected, and
    /// operates directly on the local CQRS state via aggregate commands.
    pub async fn set_position(
        pool: &SqlitePool,
        symbol: &Symbol,
        target_net: FractionalShares,
        reason: &str,
        threshold: ExecutionThreshold,
        price_usdc: Option<Float>,
    ) -> Result<SetPositionOutcome, OperatorError> {
        if reason.trim().is_empty() {
            return Err(RejectionReason::BlankReason.into());
        }

        let (position, projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .context("failed to build position store")?;

        let current = projection
            .load(symbol)
            .await
            .context("failed to load position view")?;

        if let Some(view) = &current
            && let Some(pending) = view.pending_offchain_order_id.as_ref()
        {
            return Err(RejectionReason::PositionHasPendingOrder {
                symbol: symbol.clone(),
                pending: *pending,
            }
            .into());
        }

        let previous_net = current
            .as_ref()
            .map_or(FractionalShares::ZERO, |view| view.net);

        position
            .send(
                symbol,
                PositionCommand::ManuallyAdjustPosition {
                    symbol: symbol.clone(),
                    target_net,
                    reason: reason.to_string(),
                    threshold,
                    expected_net: Some(previous_net),
                    price_usdc,
                },
            )
            .await
            .context("failed to set position")?;

        Ok(SetPositionOutcome { previous_net })
    }

    /// Fails a position's pending offchain order pointer and drives the orphaned
    /// `OffchainOrder` aggregate to `Failed`.
    ///
    /// Shared by the operator CLI and the ops API, so the bot may be driving the
    /// same order concurrently. Aggregate-first: the order is failed with
    /// `MarkFailedUnfilled`, which the aggregate evaluates against the state
    /// the store loads under its per-aggregate lock and refuses once any share
    /// has executed. A fill landing after the state read here is therefore
    /// rejected atomically (`AcquiredExecutedSharesConcurrently`) and the
    /// position pointer is left set, so the fill is accounted through the
    /// normal flow.
    pub async fn release_pending_offchain_order(
        pool: &SqlitePool,
        symbol: &Symbol,
        offchain_order_id: OffchainOrderId,
        reason: &str,
    ) -> Result<ReleaseHedgeOutcome, OperatorError> {
        if reason.trim().is_empty() {
            return Err(RejectionReason::BlankReason.into());
        }

        let (position, projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .context("failed to build position store")?;

        let Some(view) = projection
            .load(symbol)
            .await
            .context("failed to load position view")?
        else {
            return Err(RejectionReason::PositionNotFound {
                symbol: symbol.clone(),
            }
            .into());
        };

        let order = load_entity::<OffchainOrder>(pool, &offchain_order_id)
            .await
            .context("failed to load offchain order aggregate")?;

        if let Some(existing) = &order {
            if existing.symbol() != symbol {
                return Err(RejectionReason::OffchainOrderBelongsToOtherSymbol {
                    offchain_order_id,
                    owner: existing.symbol().clone(),
                    symbol: symbol.clone(),
                }
                .into());
            }
            match existing {
                OffchainOrder::PartiallyFilled { .. } => {
                    return Err(RejectionReason::OffchainOrderPartiallyFilled {
                        offchain_order_id,
                    }
                    .into());
                }
                OffchainOrder::Filled { .. } => {
                    return Err(RejectionReason::OffchainOrderFilled { offchain_order_id }.into());
                }
                OffchainOrder::Cancelling { .. } | OffchainOrder::Cancelled { .. } => {
                    return Err(RejectionReason::OffchainOrderInCancellationLifecycle {
                        offchain_order_id,
                    }
                    .into());
                }
                OffchainOrder::Pending { .. }
                | OffchainOrder::Submitted { .. }
                | OffchainOrder::Failed { .. } => {}
            }
        }

        match view.pending_offchain_order_id {
            Some(pending) if pending == offchain_order_id => {}
            Some(pending) => {
                return Err(RejectionReason::PendingPointerMismatch {
                    symbol: symbol.clone(),
                    pending,
                    offchain_order_id,
                }
                .into());
            }
            None => {
                if order.is_none() {
                    return Err(RejectionReason::NothingToRepair {
                        symbol: symbol.clone(),
                        offchain_order_id,
                    }
                    .into());
                }

                let offchain_order =
                    detail::fail_offchain_order_aggregate(pool, order, offchain_order_id, reason)
                        .await?;
                return Ok(ReleaseHedgeOutcome {
                    pointer: PointerOutcome::WasAlreadyClear,
                    offchain_order,
                });
            }
        }

        // Aggregate-first: drive the OffchainOrder to its Failed terminal before
        // clearing the position pointer. If this step escalates or fails, the
        // pointer is left intact so the pending-order safeguard still holds and a
        // concurrent fill is accounted through the normal flow. Only once the
        // order is confirmed terminal is the pointer cleared so hedging can retry.
        let offchain_order =
            detail::fail_offchain_order_aggregate(pool, order, offchain_order_id, reason).await?;

        position
            .send(
                symbol,
                PositionCommand::FailOffChainOrder {
                    offchain_order_id,
                    error: reason.to_string(),
                    // The order is confirmed terminal above; Preserve keeps the
                    // anchor so clearing the pointer cannot re-arm the double hedge.
                    anchor: AnchorDisposition::Preserve,
                    kind: OffchainOrderFailureKind::Failure,
                },
            )
            .await
            .context("failed to fail pending offchain order")?;

        Ok(ReleaseHedgeOutcome {
            pointer: PointerOutcome::ClearedNow,
            offchain_order,
        })
    }

    /// Aggregate-fail internals, kept private so the public recovery surface
    /// stays narrow; re-exported to the operator CLI's boundary tests under
    /// `test-support`.
    mod detail {
        use std::sync::Arc;

        use crate::operator::{OperatorError, RejectionReason};
        use anyhow::Context;
        use async_trait::async_trait;
        use sqlx::SqlitePool;
        use st0x_event_sorcery::{AggregateError, LifecycleError, StoreBuilder, load_entity};
        use st0x_execution::{CancellationOutcome, ExecutorOrderId, LimitOrder, MarketOrder};

        use super::OffchainOrderOutcome;
        use crate::offchain::order::{
            OffchainOrder, OffchainOrderCommand, OffchainOrderError, OffchainOrderId,
            OrderPlacementResult, OrderPlacer,
        };

        /// An [`OrderPlacer`] for repair that must never place or cancel an order.
        ///
        /// `MarkFailed` is a pure terminal transition that never touches the placer.
        /// Returns an error on the unreachable placement/cancellation paths rather
        /// than panicking.
        pub struct RepairOrderPlacer;

        #[async_trait]
        impl OrderPlacer for RepairOrderPlacer {
            async fn place_market_order(
                &self,
                _order: MarketOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                Err("repair must not place offchain orders; MarkFailed is terminal-only".into())
            }

            async fn place_limit_order(
                &self,
                _order: LimitOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                Err("repair must not place offchain orders; MarkFailed is terminal-only".into())
            }

            async fn cancel_order(
                &self,
                _executor_order_id: &ExecutorOrderId,
            ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>> {
                Err("repair must not cancel offchain orders; MarkFailed is terminal-only".into())
            }
        }

        /// How a terminal state reached concurrently (the send returned
        /// `AlreadyCompleted`) is reported.
        ///
        /// The "executed shares always escalate" rule is the load-bearing
        /// financial-safety invariant of this command; classifying the state in
        /// one place keeps the pre-send snapshot check and the post-send
        /// recovery from encoding it differently and silently diverging.
        pub enum ReloadOutcome {
            /// Executed shares present (`Filled`/`PartiallyFilled`) or a
            /// cancellation lifecycle (which may carry a partial fill): failing
            /// the order would erase a hedge the position no longer accounts
            /// for. The caller must refuse and route the operator to manual
            /// reconciliation.
            Escalate,
            /// Already `Failed`: a benign concurrent terminal transition. Report
            /// it and leave the existing failure record untouched.
            BenignTerminal,
            /// No executed shares and not terminal (`Pending`/`Submitted`/
            /// absent). Unreachable after an `AlreadyCompleted` refusal; treated
            /// as an invariant violation.
            Proceed,
        }

        /// Single source of the executed-shares-escalate rule.
        pub fn classify_reloaded_state(state: Option<&OffchainOrder>) -> ReloadOutcome {
            use OffchainOrder::{
                Cancelled, Cancelling, Failed, Filled, PartiallyFilled, Pending, Submitted,
            };

            match state {
                Some(
                    Filled { .. } | PartiallyFilled { .. } | Cancelling { .. } | Cancelled { .. },
                ) => ReloadOutcome::Escalate,
                Some(Failed { .. }) => ReloadOutcome::BenignTerminal,
                Some(Pending { .. } | Submitted { .. }) | None => ReloadOutcome::Proceed,
            }
        }

        /// Drives the standalone `OffchainOrder` aggregate (pre-loaded by the caller)
        /// to its `Failed` terminal via `MarkFailedUnfilled`.
        ///
        /// Routed through the wired store so `offchain_order_view` updates
        /// immediately. Idempotent: an already-`Failed` or absent order is reported
        /// and left untouched rather than erroring, so a partial prior run can be
        /// re-run safely; `Filled`/`PartiallyFilled` orders are refused because
        /// failing them would erase executed hedge shares.
        ///
        /// The caller's snapshot may be stale, and the bot may be driving this
        /// order concurrently (the ops API route runs in the bot process). The
        /// guard against a fill landing after the snapshot is therefore not a
        /// re-load here but the command itself: `MarkFailedUnfilled` is evaluated
        /// by the aggregate against the state `Store::send` loads under its
        /// per-aggregate lock, and refuses `PartiallyFilled` with
        /// `HasExecutedShares`. A fill in the snapshot->send gap is rejected
        /// atomically and surfaced as `AcquiredExecutedSharesConcurrently`, with
        /// the order and the position pointer left untouched.
        pub async fn fail_offchain_order_aggregate(
            pool: &SqlitePool,
            order: Option<OffchainOrder>,
            offchain_order_id: OffchainOrderId,
            reason: &str,
        ) -> Result<OffchainOrderOutcome, OperatorError> {
            use OffchainOrder::{
                Cancelled, Cancelling, Failed, Filled, PartiallyFilled, Pending, Submitted,
            };

            let Some(order) = order else {
                return Ok(OffchainOrderOutcome::NoAggregate);
            };

            match order {
                Failed { .. } => {
                    return Ok(OffchainOrderOutcome::AlreadyTerminal);
                }
                // The caller refuses executed orders before clearing the pointer;
                // refuse here too so the invariant cannot rot if a new caller skips
                // that check.
                Filled { .. } | PartiallyFilled { .. } => {
                    return Err(RejectionReason::OffchainOrderHasExecutedShares {
                        offchain_order_id,
                    }
                    .into());
                }
                Cancelling { .. } | Cancelled { .. } => {
                    return Err(RejectionReason::OffchainOrderInCancellationLifecycle {
                        offchain_order_id,
                    }
                    .into());
                }
                Pending { .. } | Submitted { .. } => {}
            }

            // The wired store (not bare send_command) so the offchain_order_view
            // projection updates immediately -- a stale 'Submitted' row in the view is
            // the very symptom this command exists to repair.
            let (store, _projection) = StoreBuilder::<OffchainOrder>::new(pool.clone())
                .build(Arc::new(RepairOrderPlacer))
                .await
                .context("failed to build offchain order store")?;
            let send_result = store
                .send(
                    &offchain_order_id,
                    OffchainOrderCommand::MarkFailedUnfilled {
                        error: reason.to_string(),
                        failed_at: chrono::Utc::now(),
                    },
                )
                .await;
            match send_result {
                Ok(()) => Ok(OffchainOrderOutcome::MarkedFailed),
                // The aggregate refused on the state it loaded under its lock: a
                // fill (or a cancellation, which may carry one) landed after the
                // caller's snapshot. The pointer is still set, so the fill is
                // accounted through the normal flow; the operator reconciles.
                Err(AggregateError::UserError(LifecycleError::Apply(
                    OffchainOrderError::HasExecutedShares { .. }
                    | OffchainOrderError::CancellationInProgress,
                ))) => {
                    Err(
                        RejectionReason::AcquiredExecutedSharesConcurrently { offchain_order_id }
                            .into(),
                    )
                }
                // A terminal state reached concurrently: a concurrent FAIL is
                // equivalent to finding it terminal up front, but a concurrent FILL
                // or cancellation means executed shares the pointer does not
                // account for -- surface that as a hard error so the operator
                // reconciles the position instead of trusting a clean exit.
                Err(AggregateError::UserError(LifecycleError::Apply(
                    OffchainOrderError::AlreadyCompleted,
                ))) => {
                    let terminal = load_entity::<OffchainOrder>(pool, &offchain_order_id)
                        .await
                        .context("failed to re-load offchain order after concurrent transition")?;
                    match classify_reloaded_state(terminal.as_ref()) {
                        // Filled or Cancelled concurrently: executed shares the
                        // cleared pointer would not account for.
                        ReloadOutcome::Escalate => {
                            Err(RejectionReason::AcquiredExecutedSharesConcurrently {
                                offchain_order_id,
                            }
                            .into())
                        }
                        ReloadOutcome::BenignTerminal => {
                            Ok(OffchainOrderOutcome::TerminalConcurrently)
                        }
                        // MarkFailedUnfilled only returns AlreadyCompleted from a
                        // terminal aggregate (Filled, Failed, or Cancelled), so a
                        // non-terminal or absent state here means the order regressed
                        // out of a terminal state -- impossible under the append-only
                        // lifecycle. Bail loudly as an invariant violation rather than
                        // silently reporting a clean "left as-is".
                        ReloadOutcome::Proceed => Err(OperatorError::Operational(anyhow::anyhow!(
                            "OffchainOrder {offchain_order_id} returned AlreadyCompleted from \
                             MarkFailedUnfilled but re-loaded as a non-terminal state -- \
                             aggregate lifecycle invariant violated"
                        ))),
                    }
                }
                // The bot's own store appended an event between this store's load
                // and its append (the two stores hold separate per-aggregate
                // locks; the event sequence is what serializes them). Nothing was
                // written. Classify what landed: executed shares escalate, a
                // concurrent fail is benign, and a still-unfilled advance
                // (Pending -> Submitted) is safe to re-run.
                Err(AggregateError::AggregateConflict) => {
                    let current = load_entity::<OffchainOrder>(pool, &offchain_order_id)
                        .await
                        .context("failed to re-load offchain order after a concurrent append")?;
                    match classify_reloaded_state(current.as_ref()) {
                        ReloadOutcome::Escalate => {
                            Err(RejectionReason::AcquiredExecutedSharesConcurrently {
                                offchain_order_id,
                            }
                            .into())
                        }
                        ReloadOutcome::BenignTerminal => {
                            Ok(OffchainOrderOutcome::TerminalConcurrently)
                        }
                        ReloadOutcome::Proceed => {
                            Err(RejectionReason::OffchainOrderChangedConcurrently {
                                offchain_order_id,
                            }
                            .into())
                        }
                    }
                }
                Err(error) => Err(OperatorError::Operational(
                    anyhow::Error::new(error).context("failed to mark offchain order failed"),
                )),
            }
        }
    }

    /// Exposes the aggregate-fail internals to the operator CLI's
    /// application-boundary tests, which drive concurrent-transition races the
    /// public entry point cannot stage deterministically.
    #[cfg(feature = "test-support")]
    pub use self::detail::{
        ReloadOutcome, RepairOrderPlacer, classify_reloaded_state, fail_offchain_order_aggregate,
    };

    #[cfg(test)]
    mod tests {
        use st0x_event_sorcery::{load_entity, send_command};
        use st0x_execution::{
            ClientOrderId, Direction, ExecutorOrderId, FractionalShares, MarketSession,
            SupportedExecutor, Symbol,
        };
        use st0x_finance::{Positive, Usd};
        use st0x_float_macro::float;
        use uuid::Uuid;

        use super::OffchainOrderOutcome;
        use super::detail::fail_offchain_order_aggregate;
        use crate::offchain::order::{
            CounterTradeOrderKind, OffchainOrder, OffchainOrderCommand, OffchainOrderId,
            noop_order_placer,
        };
        use crate::operator::{OperatorError, RejectionReason};
        use crate::test_utils::{setup_test_db, try_positive_shares};

        fn positive_shares(value: &str) -> Positive<FractionalShares> {
            try_positive_shares(value).expect("test shares must be valid and positive")
        }

        /// Seeds an `OffchainOrder` to the non-terminal `Submitted` state (Place
        /// then MarkAccepted via the noop placer), the state the aggregate-fail
        /// repair drives to `Failed`.
        async fn seed_submitted_order(
            pool: &sqlx::SqlitePool,
            order_id: OffchainOrderId,
            symbol: &Symbol,
        ) {
            send_command::<OffchainOrder>(
                pool,
                &order_id,
                OffchainOrderCommand::Place {
                    symbol: symbol.clone(),
                    shares: positive_shares("0.5"),
                    direction: Direction::Sell,
                    executor: SupportedExecutor::AlpacaBrokerApi,
                    client_order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
                    kind: CounterTradeOrderKind::Market,
                },
                noop_order_placer(),
            )
            .await
            .unwrap();
            send_command::<OffchainOrder>(
                pool,
                &order_id,
                OffchainOrderCommand::MarkAccepted {
                    executor_order_id: ExecutorOrderId::new("seed-accept"),
                    placed_shares: positive_shares("0.5"),
                    submitted_at: chrono::Utc::now(),
                    market_session: MarketSession::Regular,
                    limit_price: None,
                },
                noop_order_placer(),
            )
            .await
            .unwrap();
        }

        /// A fill landing between the caller's snapshot and the pre-send re-load
        /// must escalate: failing the order would erase executed hedge shares the
        /// position no longer accounts for.
        #[tokio::test]
        async fn refuses_to_fail_an_order_filled_since_the_snapshot() {
            let pool = setup_test_db().await;
            let symbol = Symbol::new("MSTR").unwrap();
            let order_id = OffchainOrderId::new();
            seed_submitted_order(&pool, order_id, &symbol).await;

            let stale = load_entity::<OffchainOrder>(&pool, &order_id)
                .await
                .unwrap();

            send_command::<OffchainOrder>(
                &pool,
                &order_id,
                OffchainOrderCommand::CompleteFill {
                    price: Usd::new(float!(100)),
                    filled_at: chrono::Utc::now(),
                },
                noop_order_placer(),
            )
            .await
            .unwrap();

            let error = fail_offchain_order_aggregate(&pool, stale, order_id, "operator repair")
                .await
                .unwrap_err();
            assert!(
                matches!(
                    error,
                    OperatorError::Rejected(
                        RejectionReason::AcquiredExecutedSharesConcurrently { .. }
                    )
                ),
                "expected the concurrent-execution refusal; got: {error}"
            );
        }

        /// A concurrent FAIL between the snapshot and the send is equivalent to
        /// finding the order terminal up front: a benign terminal outcome that
        /// leaves the existing failure record untouched.
        #[tokio::test]
        async fn tolerates_an_order_failed_since_the_snapshot() {
            let pool = setup_test_db().await;
            let symbol = Symbol::new("MSTR").unwrap();
            let order_id = OffchainOrderId::new();
            seed_submitted_order(&pool, order_id, &symbol).await;

            let stale = load_entity::<OffchainOrder>(&pool, &order_id)
                .await
                .unwrap();

            send_command::<OffchainOrder>(
                &pool,
                &order_id,
                OffchainOrderCommand::MarkFailed {
                    error: "bot failed it concurrently".to_string(),
                    filled_shares: None,
                    failed_at: chrono::Utc::now(),
                },
                noop_order_placer(),
            )
            .await
            .unwrap();

            let outcome = fail_offchain_order_aggregate(&pool, stale, order_id, "operator repair")
                .await
                .unwrap();
            assert_eq!(outcome, OffchainOrderOutcome::TerminalConcurrently);
        }
    }
}

/// In-bot transaction processing: account a missed on-chain fill and place the
/// opposite hedge, shared by the operator CLI and the ops API.
pub mod process_tx {
    use std::sync::Arc;
    use std::time::Duration;

    use alloy::primitives::TxHash;
    use alloy::providers::Provider;
    use anyhow::Context;
    use rain_math_float::Float;
    use sqlx::SqlitePool;
    use tokio::sync::Mutex;
    use tracing::{error, info, warn};

    use st0x_config::{Ctx, HedgedChain};
    use st0x_event_sorcery::{Projection, Store, StoreBuilder};
    use st0x_evm::{Chain, ReadOnlyEvm};
    use st0x_execution::{
        BuyingPowerReservationCents, ClientOrderId, CounterTradePreflight, CounterTradeReservation,
        Direction, FractionalShares, MarketOrder, MockExecutor, Positive, SupportedExecutor,
        Symbol,
    };
    use st0x_registry::SymbolCache;

    use crate::conductor::{
        FillAccountingOutcome, account_for_onchain_fill, execute_mark_acknowledged,
        execute_settle_fill, is_expected_place_offchain_order_rejection,
    };
    use crate::offchain::order::{
        OffchainOrder, OffchainOrderCommand, OffchainOrderFailureKind, OffchainOrderId,
        OffchainOrderPlacement, OrderPlacer, PlaceOffchainOrderError, PlacementProvenance,
        PollOrderStatusJobQueue, TerminalPositionFinalization, client_order_id_for_placement,
        place_offchain_order_at_broker, position_command_for_finalization, push_poll_job_if_absent,
        terminal_position_finalization,
    };
    use crate::onchain::accumulator::{ExecutionCtx, check_execution_readiness};
    use crate::onchain::trade::{BotOperator, RecoveryActors};
    use crate::onchain::{OnChainError, OnchainTrade, TradeValidationError};
    use crate::onchain_trade::{OnChainTrade, OnChainTradeId};
    use crate::position::{AnchorDisposition, Position, PositionCommand};
    use crate::trading::offchain::hedge::{
        BuyingPowerReservationError, acquire_counter_trade_submission_file_lock,
        live_buying_power_reservations,
    };

    use super::{OperatorError, RejectionReason};

    /// The state of a hedge order after (attempted) broker placement, or of an
    /// existing pending hedge found before placement.
    #[derive(Debug, Clone, Copy)]
    enum HedgeDisposition {
        /// The broker accepted the order; the next order-status recovery sweep
        /// reconciles it to a terminal state.
        InFlight,
        /// Placement failed or the order vanished; the position's pending marker
        /// was cleared so the normal pipeline can re-hedge.
        ClearedForRetry,
        /// The order reached a terminal broker state and the position was
        /// finalized.
        Finalized,
        /// The live pipeline deferred its own placement and is holding the order
        /// Pending with the claim set until admission permits its retry. Only the
        /// gate before placement returns this; process-tx settles its fill against
        /// that retained intent and reports the deferral without placing over it.
        Deferred,
    }

    /// Decoded on-chain fill identity and economics reported to operators.
    #[derive(Debug, Clone)]
    pub struct ProcessTxFill {
        pub tx_hash: TxHash,
        pub log_index: u64,
        pub symbol: Symbol,
        pub direction: Direction,
        pub quantity: FractionalShares,
        pub price: Float,
    }

    impl From<&OnchainTrade> for ProcessTxFill {
        fn from(trade: &OnchainTrade) -> Self {
            Self {
                tx_hash: trade.tx_hash,
                log_index: trade.log_index,
                symbol: trade.symbol().clone(),
                direction: trade.direction,
                quantity: trade.amount,
                price: trade.price(),
            }
        }
    }

    /// The two dispositions a placed process-tx hedge can carry.
    ///
    /// The broker accepted it and it is still in flight, or it reached a
    /// terminal broker state and the position was finalized. A cleared
    /// placement is not reachable here -- `finalize_hedge_outcome` routes
    /// `ClearedForRetry` to `HedgePlacementCleared` instead.
    #[derive(Debug, Clone, Copy, serde::Serialize)]
    #[serde(rename_all = "snake_case")]
    pub enum PlacedHedgeDisposition {
        /// The broker accepted the order; the next order-status recovery sweep
        /// reconciles it to a terminal state.
        InFlight,
        /// The order reached a terminal broker state and the position was
        /// finalized.
        Finalized,
    }

    /// What processing a transaction's fill resolved to.
    #[derive(Debug)]
    pub enum ProcessTxOutcome {
        /// No orderbook events in the transaction matched the configured order.
        NoTradeableEvents,
        /// The selected chain's RPC endpoint did not find the transaction.
        TransactionNotFound { tx_hash: TxHash, chain: Chain },
        /// The fill was already fully accounted; nothing to do.
        AlreadyAccounted,
        /// An existing pending hedge is in flight, so the fill was settled
        /// without placing a new hedge.
        PendingHedgeInFlight,
        /// The fill was accounted but net exposure is below the execution
        /// threshold, so no hedge was placed yet.
        BelowExecutionThreshold,
        /// Trading is disabled by configuration for the symbol; the fill was
        /// settled without placing a hedge.
        TradingDisabled { symbol: Symbol },
        /// A concurrent placement already claimed the position, so the fill was
        /// settled without placing a hedge.
        PlacementRejected { symbol: Symbol },
        /// The fill was accounted, but the placement preflight deferred the
        /// hedge: buying power could not cover a buy, or the equity reservation
        /// blocked a sell. The fill was settled without placing a hedge.
        PreflightDeferred { symbol: Symbol },
        /// A hedge order was placed at the broker and either accepted (still in
        /// flight) or reconciled to a terminal broker state.
        HedgePlaced {
            symbol: Symbol,
            offchain_order_id: OffchainOrderId,
            shares: Positive<FractionalShares>,
            direction: Direction,
            disposition: PlacedHedgeDisposition,
        },
        /// Broker placement failed or the just-placed order vanished, so the
        /// position's pending marker was cleared for the normal pipeline to
        /// re-hedge. No hedge is in flight; the fill was still accounted.
        HedgePlacementCleared { symbol: Symbol },
        /// process-tx cleared its own broker-admission-deferred placement (ADR
        /// 0022): the fill was settled, the never-sent Pending order was failed,
        /// and the position claim was cleared. No hedge from this run is in
        /// flight; the standing CheckPositions pipeline re-hedges the remaining
        /// exposure from a fresh preflight.
        HedgePlacementDeferred { symbol: Symbol },
        /// The live pipeline is already holding a deferred `Pending` hedge for
        /// its own retry, so process-tx settled the fill against that retained
        /// intent and placed no second hedge over it. The claim is preserved and
        /// the live pipeline still owns retrying it once admission permits.
        PendingHedgeDeferred {
            symbol: Symbol,
            offchain_order_id: OffchainOrderId,
        },
    }

    /// The decoded fill, when one was found, and its processing outcome.
    #[derive(Debug)]
    pub struct ProcessTxReport {
        pub fill: Option<ProcessTxFill>,
        pub outcome: ProcessTxOutcome,
    }

    /// The stores a process-tx writes through, plus the trading-schedule flag
    /// that gates how a retained pending hedge is classified.
    ///
    /// In the bot process these are the conductor's wired stores, so every
    /// event the fill produces reaches the running reactors: the
    /// `RebalancingService` applies the fill to its inventory and arms the
    /// pending-order gate immediately, rather than after the next inventory
    /// poll. The offline CLI has no reactors to reach and builds standalone
    /// stores with default projections.
    ///
    /// `schedule_enabled` mirrors the live pipeline's
    /// `CloseFlattenPolicy::schedule_enabled`: only a schedule-enabled process
    /// treats a pre-placement `Pending` order as a legitimate deferred retry.
    /// Both processes derive it from the same configuration through
    /// `trading_schedule::schedule_enabled`, so an offline CLI run
    /// classifies a leftover pending hedge exactly as the bot would.
    #[derive(Clone)]
    pub struct ProcessTxStores {
        pub onchain_trade: Arc<Store<OnChainTrade>>,
        pub position: Arc<Store<Position>>,
        pub position_projection: Arc<Projection<Position>>,
        pub offchain_order: Arc<Store<OffchainOrder>>,
        pub schedule_enabled: bool,
    }

    impl ProcessTxStores {
        /// Standalone stores with default projections and no reactors, for a
        /// process with no running bot to dispatch to. The trading schedule
        /// flag comes from `ctx`, the same configuration the conductor derives
        /// it from.
        pub async fn standalone(
            pool: &SqlitePool,
            ctx: &Ctx,
            order_placer: Arc<dyn OrderPlacer>,
        ) -> anyhow::Result<Self> {
            let (onchain_trade, _) = StoreBuilder::<OnChainTrade>::new(pool.clone())
                .build(())
                .await
                .context("failed to build onchain trade store")?;
            let (position, position_projection) = StoreBuilder::<Position>::new(pool.clone())
                .build(())
                .await
                .context("failed to build position store")?;
            let (offchain_order, _) = StoreBuilder::<OffchainOrder>::new(pool.clone())
                .build(order_placer)
                .await
                .context("failed to build offchain order store")?;
            Ok(Self {
                onchain_trade,
                position,
                position_projection,
                offchain_order,
                schedule_enabled: crate::trading_schedule::schedule_enabled(ctx),
            })
        }
    }

    /// The per-chain inputs needed to fetch and decode the target transaction:
    /// the selected hedged-chain configuration, its matching RPC provider, and
    /// the symbol cache the decoder resolves tickers through.
    pub struct ProcessTxChainContext<'a, P> {
        trading_chain: &'a HedgedChain,
        provider: &'a P,
        cache: &'a SymbolCache,
    }

    impl<'a, P> ProcessTxChainContext<'a, P> {
        /// Couples the chain decoder configuration to the provider and symbol
        /// cache the caller selected for that chain.
        pub const fn new(
            trading_chain: &'a HedgedChain,
            provider: &'a P,
            cache: &'a SymbolCache,
        ) -> Self {
            Self {
                trading_chain,
                provider,
                cache,
            }
        }
    }

    /// Guards a decoded fill against a chain other than the one the request
    /// selected. The decoder is fed the requested chain, so a divergence is an
    /// invariant break: refuse rather than hedge a fill from an unselected chain.
    fn ensure_decoded_chain_matches(
        decoded: Chain,
        requested: Chain,
    ) -> Result<(), RejectionReason> {
        if decoded == requested {
            Ok(())
        } else {
            Err(RejectionReason::DecodedChainMismatch { requested, decoded })
        }
    }

    /// Accounts a missed on-chain fill from `tx_hash` and, when the resulting
    /// net exposure warrants it, places the opposite hedge.
    ///
    /// Run inside the bot process, pass a [`ProcessTxChainContext`] containing
    /// the selected hedged-chain config and its provider, the conductor's wired
    /// `stores` (so the fill reaches the running reactors), and the live
    /// `submission_lock` so the pending-hedge inspection and the broker
    /// placement serialize against the trading loop (ADR 0014). Under the lock,
    /// the shared `Position` aggregate's pending-order gate prevents a racing
    /// tick from double-placing the hedge, and passes `Some` poll enrollment so
    /// a newly submitted hedge is enqueued for status polling under the guards,
    /// mirroring the live placement path. The CLI runs in a separate process
    /// with standalone stores, no shared lock, and passes `None` for both,
    /// relying on startup recovery to enrol its submitted orders.
    pub async fn process_tx<P: Provider + Clone + 'static>(
        tx_hash: TxHash,
        ctx: &Ctx,
        pool: &SqlitePool,
        chain: ProcessTxChainContext<'_, P>,
        stores: &ProcessTxStores,
        order_placer: Arc<dyn OrderPlacer>,
        submission_lock: Option<&Mutex<()>>,
        poll_enrollment: Option<(&PollOrderStatusJobQueue, Duration)>,
    ) -> Result<ProcessTxReport, OperatorError> {
        let ProcessTxChainContext {
            trading_chain,
            provider,
            cache,
        } = chain;
        let actors = RecoveryActors {
            order_owner: trading_chain.vault_owner,
            bot_operator: BotOperator(ctx.order_owner()),
        };
        let read_evm = ReadOnlyEvm::new(provider.clone());

        match OnchainTrade::try_from_tx_hash(tx_hash, &read_evm, cache, trading_chain, actors).await
        {
            Ok(Some(onchain_trade)) => {
                // The decoder is fed the requested chain, so a decoded fill on a
                // different chain is an invariant break: refuse rather than hedge
                // a fill from a chain the operator did not select.
                ensure_decoded_chain_matches(onchain_trade.chain, trading_chain.chain)?;
                let fill = ProcessTxFill::from(&onchain_trade);
                let outcome = process_found_trade(
                    onchain_trade,
                    ctx,
                    pool,
                    stores,
                    order_placer,
                    submission_lock,
                    poll_enrollment,
                )
                .await?;
                Ok(ProcessTxReport {
                    fill: Some(fill),
                    outcome,
                })
            }
            Ok(None) => Ok(ProcessTxReport {
                fill: None,
                outcome: ProcessTxOutcome::NoTradeableEvents,
            }),
            Err(OnChainError::Validation(TradeValidationError::TransactionNotFound(_))) => {
                Ok(ProcessTxReport {
                    fill: None,
                    outcome: ProcessTxOutcome::TransactionNotFound {
                        tx_hash,
                        chain: trading_chain.chain,
                    },
                })
            }
            Err(error) => Err(OperatorError::Operational(anyhow::Error::new(error))),
        }
    }

    /// Accounts a decoded fill, reconciles any pending hedge, and places a new hedge when needed.
    async fn process_found_trade(
        onchain_trade: OnchainTrade,
        ctx: &Ctx,
        pool: &SqlitePool,
        stores: &ProcessTxStores,
        order_placer: Arc<dyn OrderPlacer>,
        submission_lock: Option<&Mutex<()>>,
        poll_enrollment: Option<(&PollOrderStatusJobQueue, Duration)>,
    ) -> Result<ProcessTxOutcome, OperatorError> {
        let trade_id = OnChainTradeId::new(
            onchain_trade.chain,
            onchain_trade.tx_hash,
            onchain_trade.log_index,
        );
        let trading_chain = ctx
            .chains
            .hedged_chain(onchain_trade.chain)
            .with_context(|| {
                format!(
                    "process-tx decoded a fill on {}, which is not configured as a hedged chain",
                    onchain_trade.chain
                )
            })?;

        let ProcessTxStores {
            onchain_trade: onchain_trade_store,
            position: position_store,
            ..
        } = stores;
        let Some(block_number) = onchain_trade.block_number else {
            return Err(RejectionReason::FillMissingBlockNumber { trade_id }.into());
        };

        let FillAccountingOutcome::Accounted { trade_id } = account_for_onchain_fill(
            pool,
            onchain_trade_store,
            position_store,
            &onchain_trade,
            block_number,
            ctx.execution_threshold,
        )
        .await
        .context("failed to account for the onchain fill")?
        else {
            return Ok(ProcessTxOutcome::AlreadyAccounted);
        };

        let base_symbol = onchain_trade.symbol();

        // Serialize against the live trading loop (ADR 0014) from here on.
        // The lock must cover the pending-hedge inspection below, not just
        // the placement: a concurrent placement holds the lock across
        // `Position::PlaceOffChainOrder` (the claim) and `OffchainOrder::Place`
        // (the aggregate), so an absent aggregate observed under the lock is a
        // genuine orphan, whereas one observed outside it may be a live claim
        // whose aggregate is about to exist. Held only on the in-bot path;
        // released when this scope ends.
        let _submission_guard = match submission_lock {
            Some(lock) => Some(lock.lock().await),
            None => None,
        };
        // The in-process mutex only serializes within this process, and the CLI
        // passes `None`. The pool-scoped file lock is the only guard that crosses
        // process boundaries; the bot's own `process_queued_trade` and
        // `PlaceHedge` worker take it in the same mutex-then-file order, so an
        // operator running process-tx against a live bot cannot submit inside the
        // window between the bot's preflight and its `Placed` event.
        let _file_submission_guard = acquire_counter_trade_submission_file_lock(pool)
            .await
            .context("failed to acquire the counter-trade submission file lock")?;

        let params = match gate_fill_for_placement(
            ctx,
            stores,
            trading_chain,
            &onchain_trade,
            &trade_id,
            base_symbol,
            poll_enrollment,
        )
        .await?
        {
            FillGate::Settled(outcome) => return Ok(outcome),
            FillGate::Ready(params) => params,
        };

        place_ready_hedge(
            ctx,
            pool,
            stores,
            order_placer,
            params,
            &trade_id,
            &onchain_trade,
            poll_enrollment,
        )
        .await
    }

    /// Places the hedge for a fill the gate marked ready: reconciles a preserved
    /// failed-order anchor, runs the placement preflight, claims the position,
    /// places the order at the broker, and resolves the post-placement
    /// disposition. Each early exit settles the fill; the in-flight success path
    /// enrolls the poll job before settling so a failed enqueue leaves the fill
    /// unsettled for a retry. Runs under the caller's still-held submission
    /// guards.
    async fn place_ready_hedge(
        ctx: &Ctx,
        pool: &SqlitePool,
        stores: &ProcessTxStores,
        order_placer: Arc<dyn OrderPlacer>,
        params: ExecutionCtx,
        trade_id: &OnChainTradeId,
        onchain_trade: &OnchainTrade,
        poll_enrollment: Option<(&PollOrderStatusJobQueue, Duration)>,
    ) -> Result<ProcessTxOutcome, OperatorError> {
        let ProcessTxStores {
            onchain_trade: onchain_trade_store,
            position: position_store,
            offchain_order: offchain_order_store,
            ..
        } = stores;
        let offchain_order_id = OffchainOrderId::new();

        let anchor = match reconcile_failed_anchor(
            position_store,
            order_placer.as_ref(),
            &params.symbol,
            offchain_order_id,
            params.executor,
        )
        .await
        {
            Ok(anchor) => anchor,
            Err(error) => {
                // The fill must be durably settled before a typed rejection is
                // returned: the rejection reports the fill as handled, so
                // accounting left behind it would never be retried. A settle
                // failure therefore propagates as an operational failure and
                // leaves the fill unacknowledged for a retry, and only a
                // settle that succeeded lets the captured error surface -- a
                // typed rejection when a broker order still holds the anchor,
                // an operational store or RPC failure otherwise.
                mark_and_settle_fill(onchain_trade_store, position_store, trade_id, onchain_trade)
                    .await?;
                return Err(error);
            }
        };

        let client_order_id = client_order_id_for_placement(offchain_order_id, anchor);

        // The submission guards above are still held here, so the placement
        // preflight, the aggregate claim, and the broker placement all
        // serialize against the trading loop. The preflight runs for both
        // directions: a buy reserves cash buying power, a sell reserves equity
        // inventory against the hedge floor and non fractionable sizing.
        let Some((hedge_shares, buying_power_reservation)) = preflight_placement(
            pool,
            order_placer.as_ref(),
            &params.symbol,
            params.shares,
            params.direction,
            client_order_id.clone(),
            params.executor,
        )
        .await?
        else {
            mark_and_settle_fill(onchain_trade_store, position_store, trade_id, onchain_trade)
                .await?;
            return Ok(ProcessTxOutcome::PreflightDeferred {
                symbol: params.symbol.clone(),
            });
        };

        match position_store
            .send(
                &params.symbol,
                PositionCommand::PlaceOffChainOrder {
                    offchain_order_id,
                    shares: hedge_shares,
                    direction: params.direction,
                    executor: params.executor,
                    threshold: ctx.execution_threshold,
                },
            )
            .await
        {
            Ok(()) => {}
            Err(error) if is_expected_place_offchain_order_rejection(&error) => {
                info!(
                    %offchain_order_id,
                    symbol = %params.symbol,
                    "Position::PlaceOffChainOrder rejected by domain state: {error}"
                );
                mark_and_settle_fill(onchain_trade_store, position_store, trade_id, onchain_trade)
                    .await?;
                return Ok(ProcessTxOutcome::PlacementRejected {
                    symbol: params.symbol.clone(),
                });
            }
            Err(error) => return Err(OperatorError::Operational(anyhow::Error::new(error))),
        }

        match place_offchain_order_at_broker(
            offchain_order_store,
            order_placer.as_ref(),
            &offchain_order_id,
            OffchainOrderPlacement::market(
                params.symbol.clone(),
                hedge_shares,
                params.direction,
                params.executor,
                client_order_id,
            )
            .with_buying_power_reservation(buying_power_reservation)
            .with_provenance(PlacementProvenance::ProcessTx),
        )
        .await
        {
            Ok(_) => {}
            // Broker admission deferred this attempt under the schedule aware
            // close flatten policy (ADR 0022). Option 2: do NOT retain the
            // Pending intent. Fail the still Pending order and clear the position
            // claim to the retry eligible state so the normal CheckPositions
            // pipeline detects the unhedged exposure again and preflights a fresh
            // hedge from scratch, replaying no stale shares or reservation terms.
            // A deferral returns before the broker call, so the retired id is
            // released rather than left as an anchor for an order that was never
            // created.
            Err(PlaceOffchainOrderError::Deferred) => {
                fail_unsent_placement(
                    stores,
                    &params.symbol,
                    offchain_order_id,
                    trade_id,
                    onchain_trade,
                    "process-tx placement deferred by broker admission".to_owned(),
                    OffchainOrderFailureKind::Deferral,
                    AnchorDisposition::Release,
                )
                .await?;
                return Ok(ProcessTxOutcome::HedgePlacementDeferred {
                    symbol: params.symbol.clone(),
                });
            }
            // Neither an admission failure nor backpressure left an order the
            // broker accepted, so both clear the claim to the retry eligible
            // state, settle the fill, and surface the operational failure with
            // the real cause recorded on the durable events. They differ in how
            // far the attempt got: an admission failure returns before the broker
            // call, so its id is released, while backpressure means the broker
            // WAS contacted and may have created an order, so its id is preserved
            // as the anchor that forces the next attempt through
            // reconcile_failed_anchor. The clearing itself must not mask the
            // broker failure: a settle failure is logged and the original error
            // still wins, so a backpressure classification survives for the
            // caller's retry policy. A store/persistence error (`Command`) leaves
            // the order state uncertain, so that one keeps the plain operational
            // return without touching the claim.
            Err(PlaceOffchainOrderError::Admission { source }) => {
                // The wrapper's own `Display` is not usable as the durable
                // reason: thiserror never walks `#[source]`, so it would drop
                // the admission cause, and its canned text claims the pending
                // intent is retained, the opposite of what this path does.
                let reason = format!(
                    "process-tx admission failure; claim cleared, order not sent to broker: {}",
                    render_cause_chain(&*source)
                );
                if let Err(clear_error) = fail_unsent_placement(
                    stores,
                    &params.symbol,
                    offchain_order_id,
                    trade_id,
                    onchain_trade,
                    reason,
                    OffchainOrderFailureKind::Failure,
                    AnchorDisposition::Release,
                )
                .await
                {
                    error!(
                        %offchain_order_id,
                        symbol = %params.symbol,
                        %clear_error,
                        "Failed to clear the unsent placement after a broker admission failure"
                    );
                }
                return Err(
                    anyhow::Error::new(PlaceOffchainOrderError::Admission { source })
                        .context("failed to place the offchain order at the broker")
                        .into(),
                );
            }
            Err(PlaceOffchainOrderError::Backpressure { source }) => {
                let reason = format!(
                    "process-tx backpressure; claim cleared, failed order id preserved as \
                     idempotency anchor: {}",
                    render_cause_chain(&*source)
                );
                if let Err(clear_error) = fail_unsent_placement(
                    stores,
                    &params.symbol,
                    offchain_order_id,
                    trade_id,
                    onchain_trade,
                    reason,
                    OffchainOrderFailureKind::Failure,
                    AnchorDisposition::Preserve,
                )
                .await
                {
                    error!(
                        %offchain_order_id,
                        symbol = %params.symbol,
                        %clear_error,
                        "Failed to clear the unsent placement after broker backpressure"
                    );
                }
                return Err(
                    anyhow::Error::new(PlaceOffchainOrderError::Backpressure { source })
                        .context("failed to place the offchain order at the broker")
                        .into(),
                );
            }
            Err(error) => {
                return Err(anyhow::Error::new(error)
                    .context("failed to place the offchain order at the broker")
                    .into());
            }
        }

        let disposition = reconcile_post_place_state(
            offchain_order_store,
            position_store,
            &params.symbol,
            offchain_order_id,
        )
        .await?;

        // Resolve the disposition first: for an in-flight hedge this enrolls the
        // poll job. Settle the fill only after that succeeds, mirroring the
        // gate's existing-in-flight branch: a failed enqueue returns here and
        // leaves the fill unsettled so the retry re-enrolls then settles, rather
        // than settling first and stranding the poll job when the retry short
        // circuits as AlreadyAccounted.
        let outcome = finalize_hedge_outcome(
            disposition,
            &params.symbol,
            offchain_order_id,
            hedge_shares,
            params.direction,
            poll_enrollment,
        )
        .await?;

        mark_and_settle_fill(onchain_trade_store, position_store, trade_id, onchain_trade).await?;

        Ok(outcome)
    }

    /// Outcome of gating a decoded fill before placement: either the fill was
    /// settled with a terminal outcome, or the position is ready and yields the
    /// placement parameters.
    enum FillGate {
        Settled(ProcessTxOutcome),
        Ready(ExecutionCtx),
    }

    /// Gates a decoded fill before placement: reconciles any live pending hedge,
    /// honours the trading-enabled flag, and checks execution readiness. Each
    /// gate settles the fill and reports its terminal outcome; a ready position
    /// yields the placement parameters. Runs under the caller's submission
    /// guards so the readiness decision cannot race the live trading loop.
    async fn gate_fill_for_placement(
        ctx: &Ctx,
        stores: &ProcessTxStores,
        trading_chain: &HedgedChain,
        onchain_trade: &OnchainTrade,
        trade_id: &OnChainTradeId,
        base_symbol: &Symbol,
        poll_enrollment: Option<(&PollOrderStatusJobQueue, Duration)>,
    ) -> Result<FillGate, OperatorError> {
        let ProcessTxStores {
            onchain_trade: onchain_trade_store,
            position: position_store,
            position_projection,
            offchain_order: offchain_order_store,
            schedule_enabled,
        } = stores;

        match reconcile_existing_pending_order(offchain_order_store, position_store, base_symbol)
            .await?
        {
            None | Some((_, HedgeDisposition::ClearedForRetry | HedgeDisposition::Finalized)) => {}
            Some((pending_offchain_order_id, HedgeDisposition::InFlight)) => {
                // Enroll the still live hedge for status polling before settling
                // the fill, mirroring the live placement path: an existing
                // Submitted/PartiallyFilled/Cancelling order that is not enrolled
                // would sit unpolled until the next startup recovery sweep.
                // Enrollment must succeed before the fill settles: were the fill
                // settled first and the enqueue then failed, the retry would
                // short circuit as AlreadyAccounted and never restore the poll
                // job. Leaving the fill unsettled lets the retry resume and run
                // enrollment again; the enqueue is guarded against duplicates, so
                // running it again after a partial success adds no duplicate.
                if let Some((poll_status_queue, poll_interval)) = poll_enrollment {
                    push_poll_job_if_absent(
                        poll_status_queue.clone(),
                        pending_offchain_order_id,
                        poll_interval,
                    )
                    .await
                    .inspect_err(|error| {
                        error!(
                            %pending_offchain_order_id,
                            symbol = %base_symbol,
                            %error,
                            "Failed to enqueue PollOrderStatus for the existing in-flight hedge"
                        );
                    })
                    .map_err(|error| OperatorError::Operational(anyhow::Error::new(error)))?;
                }
                mark_and_settle_fill(onchain_trade_store, position_store, trade_id, onchain_trade)
                    .await?;
                return Ok(FillGate::Settled(ProcessTxOutcome::PendingHedgeInFlight));
            }
            Some((pending_offchain_order_id, HedgeDisposition::Deferred)) if *schedule_enabled => {
                // A schedule-enabled live pipeline deferred its own placement and
                // is holding this order Pending with the claim set until admission
                // permits its retry (process-tx no longer retains a Pending; ADR
                // 0022). The later fill has already been applied to the position
                // by account_for_onchain_fill; settle it against that retained
                // intent and report the deferral rather than placing a second
                // hedge over it. The live pipeline still owns retrying its Pending
                // order once admission permits.
                mark_and_settle_fill(onchain_trade_store, position_store, trade_id, onchain_trade)
                    .await?;
                return Ok(FillGate::Settled(ProcessTxOutcome::PendingHedgeDeferred {
                    symbol: base_symbol.clone(),
                    offchain_order_id: pending_offchain_order_id,
                }));
            }
            Some((pending_offchain_order_id, HedgeDisposition::Deferred)) => {
                // Schedule disabled: a Pending order is not a legitimate deferred
                // resting state (matching the live path's strict schedule_enabled
                // check). The fill must be durably settled before the typed
                // rejection is returned, because the rejection reports the fill
                // as handled and accounting left behind it would never be
                // retried. A settle failure therefore propagates as an
                // operational failure and leaves the fill unacknowledged for a
                // retry; only after the settle succeeds does the rejection
                // surface, preserving the claim instead of placing a second
                // hedge over an order that was never sent.
                mark_and_settle_fill(onchain_trade_store, position_store, trade_id, onchain_trade)
                    .await?;
                return Err(RejectionReason::RetainedPendingWithoutSchedule {
                    offchain_order_id: pending_offchain_order_id,
                    symbol: base_symbol.clone(),
                }
                .into());
            }
        }

        let trading_enabled = trading_chain.assets.is_trading_enabled(base_symbol);

        if !trading_enabled {
            mark_and_settle_fill(onchain_trade_store, position_store, trade_id, onchain_trade)
                .await?;
            return Ok(FillGate::Settled(ProcessTxOutcome::TradingDisabled {
                symbol: base_symbol.clone(),
            }));
        }

        let executor_type = ctx.broker.to_supported_executor();
        // process-tx is a manual recovery verb: a `MockExecutor` forces the
        // readiness check to treat the market as open so the operator can place
        // the hedge regardless of session, matching the CLI path.
        let executor = MockExecutor::new();
        let Some(params) = check_execution_readiness(
            &executor,
            position_projection,
            base_symbol,
            executor_type,
            &trading_chain.assets,
            &ctx.assets,
            trading_enabled,
        )
        .await
        .context("failed to check execution readiness")?
        else {
            mark_and_settle_fill(onchain_trade_store, position_store, trade_id, onchain_trade)
                .await?;
            return Ok(FillGate::Settled(ProcessTxOutcome::BelowExecutionThreshold));
        };

        Ok(FillGate::Ready(params))
    }

    /// Resolves a post-placement disposition into a process-tx outcome. A
    /// submitted (`InFlight`) hedge is enrolled for status polling under the
    /// still-held submission guards, mirroring the live path. The caller settles
    /// the fill only after this returns Ok, so a failed enqueue here leaves the
    /// fill unsettled for the retry rather than settling first and stranding the
    /// poll job. A cleared placement reports that no hedge was placed, and a
    /// finalized one needs no poll job.
    async fn finalize_hedge_outcome(
        disposition: HedgeDisposition,
        symbol: &Symbol,
        offchain_order_id: OffchainOrderId,
        hedge_shares: Positive<FractionalShares>,
        direction: Direction,
        poll_enrollment: Option<(&PollOrderStatusJobQueue, Duration)>,
    ) -> Result<ProcessTxOutcome, OperatorError> {
        let placed_disposition = match disposition {
            HedgeDisposition::ClearedForRetry => {
                return Ok(ProcessTxOutcome::HedgePlacementCleared {
                    symbol: symbol.clone(),
                });
            }
            HedgeDisposition::InFlight => {
                if let Some((poll_status_queue, poll_interval)) = poll_enrollment {
                    push_poll_job_if_absent(
                        poll_status_queue.clone(),
                        offchain_order_id,
                        poll_interval,
                    )
                    .await
                    .inspect_err(|error| {
                        error!(
                            %offchain_order_id,
                            %symbol,
                            %error,
                            "Failed to enqueue PollOrderStatus for the process-tx hedge"
                        );
                    })
                    .map_err(|error| OperatorError::Operational(anyhow::Error::new(error)))?;
                }
                PlacedHedgeDisposition::InFlight
            }
            HedgeDisposition::Finalized => PlacedHedgeDisposition::Finalized,
            HedgeDisposition::Deferred => {
                // Only the gate before placement classifies a retained Pending
                // order as deferred; reconcile_post_place_state never returns it,
                // so reaching here means an invariant broke rather than a state to
                // report as a placed hedge.
                return Err(OperatorError::Operational(anyhow::anyhow!(
                    "reconciliation after placement produced a deferred disposition for {symbol}; \
                     refusing to report the hedge as placed"
                )));
            }
        };
        Ok(ProcessTxOutcome::HedgePlaced {
            symbol: symbol.clone(),
            offchain_order_id,
            shares: hedge_shares,
            direction,
            disposition: placed_disposition,
        })
    }

    /// Completes fill accounting after the recovery path has resolved its hedge decision.
    async fn mark_and_settle_fill(
        onchain_trade_store: &Store<OnChainTrade>,
        position_store: &Store<Position>,
        trade_id: &OnChainTradeId,
        onchain_trade: &OnchainTrade,
    ) -> anyhow::Result<()> {
        execute_mark_acknowledged(onchain_trade_store, trade_id).await?;
        execute_settle_fill(position_store, onchain_trade).await?;
        Ok(())
    }

    /// Renders an error together with every link of its `source()` chain.
    ///
    /// A durable failure reason has to carry the real cause, and thiserror's
    /// generated `Display` never walks `#[source]`: formatting a wrapper alone
    /// would persist its canned message and discard the broker error that
    /// explains the failure to whoever reads the event back.
    fn render_cause_chain(error: &(dyn std::error::Error + 'static)) -> String {
        std::iter::successors(Some(error), |error| error.source())
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(": ")
    }

    /// Retires a placement the broker never accepted: fails the still Pending
    /// order with `reason` and clears the position claim to the retry eligible
    /// state (ADR 0022), then settles the fill. Drives the aggregate to Failed
    /// before clearing the claim, the order the failed placement path uses. The
    /// `anchor` disposition says whether the retired id stays as the idempotency
    /// anchor: a placement that never reached the broker releases it, so no
    /// phantom anchor reconciliation is scheduled for an order the broker never
    /// saw, while a placement whose broker call did run preserves it so the next
    /// attempt reconciles whatever the broker may have created. The standing
    /// pipeline then hedges the remaining exposure again from a fresh preflight.
    ///
    /// `kind` records whether the retirement is a schedule or admission deferral
    /// or a genuine failure. The position side is identical either way -- the
    /// claim is cleared and the order goes terminal -- but only a genuine
    /// failure is counted as a hedge failure by the reliability projection.
    async fn fail_unsent_placement(
        stores: &ProcessTxStores,
        symbol: &Symbol,
        offchain_order_id: OffchainOrderId,
        trade_id: &OnChainTradeId,
        onchain_trade: &OnchainTrade,
        reason: String,
        kind: OffchainOrderFailureKind,
        anchor: AnchorDisposition,
    ) -> Result<(), OperatorError> {
        let ProcessTxStores {
            onchain_trade: onchain_trade_store,
            position: position_store,
            offchain_order: offchain_order_store,
            ..
        } = stores;
        offchain_order_store
            .send(
                &offchain_order_id,
                OffchainOrderCommand::MarkPlacementFailed {
                    error: reason.clone(),
                    kind,
                },
            )
            .await
            .map_err(|error| OperatorError::Operational(anyhow::Error::new(error)))?;
        position_store
            .send(
                symbol,
                PositionCommand::FailOffChainOrder {
                    offchain_order_id,
                    error: reason,
                    anchor,
                    kind,
                },
            )
            .await
            .context("failed to clear the unsent offchain order from the position")?;
        mark_and_settle_fill(onchain_trade_store, position_store, trade_id, onchain_trade).await?;
        Ok(())
    }

    /// Reconciles a preserved failed-order anchor with the broker before a fresh
    /// placement reuses it as the client order id. For an Alpaca executor a
    /// present broker order means the prior attempt did reach the broker, so it
    /// refuses rather than double-submitting; only a confirmed absence releases
    /// the anchor. Non-Alpaca executors have no such lookup, so the anchor is
    /// returned unchanged.
    async fn reconcile_failed_anchor(
        position_store: &Store<Position>,
        order_placer: &dyn OrderPlacer,
        symbol: &Symbol,
        offchain_order_id: OffchainOrderId,
        executor: SupportedExecutor,
    ) -> Result<Option<OffchainOrderId>, OperatorError> {
        let anchor = position_store
            .load(symbol)
            .await
            .inspect_err(|error| {
                error!(
                    %offchain_order_id,
                    %symbol,
                    %error,
                    "Failed to load position for the idempotency anchor; refusing \
                     placement until it can be read"
                );
            })
            .context("failed to load position for the idempotency anchor")?
            .and_then(|position| position.last_failed_offchain_order_id);
        if executor != SupportedExecutor::AlpacaBrokerApi {
            return Ok(anchor);
        }
        let Some(anchor) = anchor else {
            return Ok(None);
        };

        let client_order_id = client_order_id_for_placement(offchain_order_id, Some(anchor));
        if let Some(broker_order) = order_placer
            .get_order_by_client_order_id(&client_order_id)
            .await
            .map_err(anyhow::Error::from_boxed)?
        {
            return Err(RejectionReason::FailedAnchorStillAtBroker {
                anchor,
                executor_order_id: broker_order.executor_order_id,
            }
            .into());
        }
        position_store
            .send(
                symbol,
                PositionCommand::ReleaseFailedOrderAnchor {
                    expected_offchain_order_id: anchor,
                },
            )
            .await
            .context("failed to release the failed-order anchor")?;
        Ok(None)
    }

    /// A placement preflight verdict that contradicts the order it was run for.
    ///
    /// `OrderPlacer` is a type erased trait object, so nothing in the type
    /// system stops an implementation from answering a buy with an equity
    /// reservation, a sell with a buying power reservation, or a sell with
    /// equity reserved in some other symbol. Placing on such a verdict would
    /// submit a sell the broker never reserved inventory or floor room for, or
    /// a buy it never reserved cash for, so each one fails closed here as an
    /// operational failure instead of falling back to the requested size.
    #[derive(Debug, thiserror::Error)]
    pub enum PreflightReservationMismatch {
        #[error(
            "placement preflight for a buy of {symbol} returned an equity reservation; a buy \
             must reserve cash buying power, so the hedge is refused"
        )]
        BuyReservedEquity { symbol: Symbol },
        #[error(
            "placement preflight for a sell of {symbol} returned a buying power reservation; a \
             sell must reserve equity inventory, so the hedge is refused"
        )]
        SellReservedBuyingPower { symbol: Symbol },
        #[error(
            "placement preflight for a sell of {symbol} reserved equity in {reserved}; the hedge \
             is refused rather than sold against another symbol's inventory"
        )]
        SellReservedOtherSymbol { symbol: Symbol, reserved: Symbol },
        #[error(
            "placement preflight for a {direction} of {symbol} returned no reservation, but the \
             Alpaca executor always reserves; the hedge is refused rather than placed unreserved"
        )]
        AllowedWithoutReservation {
            symbol: Symbol,
            direction: Direction,
        },
    }

    /// Runs the safety preflight at placement time for either direction, mirroring
    /// the live path's `preflight_fresh_placement`: a buy prices the hedge
    /// against live cash buying power reservations, a sell reserves equity
    /// inventory against the hedge floor and non fractionable sizing. Returns
    /// `None` to defer, or the broker approved (possibly clamped) share count
    /// and the buying power reservation to attach to a buy order. A reservation
    /// that does not match the direction it was requested for fails closed with
    /// a [`PreflightReservationMismatch`].
    async fn preflight_placement(
        pool: &SqlitePool,
        order_placer: &dyn OrderPlacer,
        symbol: &Symbol,
        shares: Positive<FractionalShares>,
        direction: Direction,
        client_order_id: ClientOrderId,
        executor: SupportedExecutor,
    ) -> Result<
        Option<(
            Positive<FractionalShares>,
            Option<BuyingPowerReservationCents>,
        )>,
        OperatorError,
    > {
        let reserved = if direction == Direction::Buy {
            match live_buying_power_reservations(pool).await {
                Ok(reserved) => reserved,
                Err(BuyingPowerReservationError::Unknown { order_id }) => {
                    warn!(
                        %symbol,
                        %order_id,
                        "process-tx deferring buy hedge while a live legacy buy has unknown reserved buying power"
                    );
                    return Ok(None);
                }
                Err(error) => return Err(OperatorError::Operational(error.into())),
            }
        } else {
            BuyingPowerReservationCents::ZERO
        };
        let preflight = order_placer
            .preflight_counter_trade_with_reserved_buying_power(
                MarketOrder {
                    symbol: symbol.clone(),
                    shares,
                    direction,
                    client_order_id,
                },
                reserved,
            )
            .await
            .map_err(anyhow::Error::from_boxed)?;
        let reservation = match preflight {
            CounterTradePreflight::Skipped(reason) => {
                info!(%symbol, "process-tx placement preflight deferred the hedge: {reason}");
                return Ok(None);
            }
            CounterTradePreflight::Allowed { reservation } => reservation,
        };
        let (placed_shares, buying_power_reservation) = match (direction, reservation) {
            (
                Direction::Buy,
                Some(CounterTradeReservation::BuyingPower {
                    required,
                    estimated_cost_cents,
                    ..
                }),
            ) => (
                required,
                Some(
                    BuyingPowerReservationCents::new(estimated_cost_cents)
                        .map_err(anyhow::Error::from)?,
                ),
            ),
            (
                Direction::Sell,
                Some(CounterTradeReservation::Equity {
                    symbol: reserved_symbol,
                    required,
                    ..
                }),
            ) if &reserved_symbol == symbol => (required, None),
            (
                Direction::Sell,
                Some(CounterTradeReservation::Equity {
                    symbol: reserved_symbol,
                    ..
                }),
            ) => {
                return Err(PreflightReservationMismatch::SellReservedOtherSymbol {
                    symbol: symbol.clone(),
                    reserved: reserved_symbol,
                }
                .into());
            }
            (Direction::Buy, Some(CounterTradeReservation::Equity { .. })) => {
                return Err(PreflightReservationMismatch::BuyReservedEquity {
                    symbol: symbol.clone(),
                }
                .into());
            }
            (Direction::Sell, Some(CounterTradeReservation::BuyingPower { .. })) => {
                return Err(PreflightReservationMismatch::SellReservedBuyingPower {
                    symbol: symbol.clone(),
                }
                .into());
            }
            // An Alpaca placement always answers with a reservation or a skip,
            // so a reservation-free allow contradicts the executor it was run
            // for: refuse rather than place a hedge nothing reserved cash or
            // inventory for.
            (Direction::Buy | Direction::Sell, None)
                if executor == SupportedExecutor::AlpacaBrokerApi =>
            {
                return Err(PreflightReservationMismatch::AllowedWithoutReservation {
                    symbol: symbol.clone(),
                    direction,
                }
                .into());
            }
            // The `OrderPlacer` default allows with no reservation, which is
            // what a placer that runs no preflight of its own returns: the dry
            // run executor with no inventory to reserve against, and the
            // operator placers that never price a hedge. Nothing was reserved
            // and nothing was clamped, so the requested size stands.
            (Direction::Buy | Direction::Sell, None) => (shares, None),
        };
        Ok(Some((placed_shares, buying_power_reservation)))
    }

    /// Inspects any pending hedge already recorded on the position before
    /// placing a new one. `None` means no pending order; otherwise the returned
    /// pair carries the pending order id and whether it is still live
    /// (`InFlight`) or was resolved (`ClearedForRetry`/`Finalized`) so the
    /// caller may enroll the live order for polling or place a fresh hedge.
    async fn reconcile_existing_pending_order(
        offchain_order_store: &Store<OffchainOrder>,
        position_store: &Store<Position>,
        symbol: &Symbol,
    ) -> Result<Option<(OffchainOrderId, HedgeDisposition)>, OperatorError> {
        let Some(position) = position_store
            .load(symbol)
            .await
            .context("failed to load position")?
        else {
            return Ok(None);
        };
        let Some(offchain_order_id) = position.pending_offchain_order_id else {
            return Ok(None);
        };
        let loaded_order = offchain_order_store
            .load(&offchain_order_id)
            .await
            .inspect_err(|error| {
                error!(
                    %offchain_order_id,
                    %symbol,
                    %error,
                    "Failed to load existing pending offchain order; cannot safely acknowledge fill"
                );
            })
            .context("failed to load existing pending offchain order")?;
        reconcile_offchain_order_state(
            loaded_order,
            position_store,
            symbol,
            offchain_order_id,
            PlacementContext::PrePlacement,
        )
        .await
        .map(|disposition| Some((offchain_order_id, disposition)))
    }

    /// Mirrors dispatch_post_place_state in the normal pipeline: inspect the
    /// persisted offchain order state and clear the position pending marker on
    /// failure so the normal pipeline can re-hedge on its next cycle.
    async fn reconcile_post_place_state(
        offchain_order_store: &Store<OffchainOrder>,
        position_store: &Store<Position>,
        symbol: &Symbol,
        offchain_order_id: OffchainOrderId,
    ) -> Result<HedgeDisposition, OperatorError> {
        let loaded_order = offchain_order_store
            .load(&offchain_order_id)
            .await
            .inspect_err(|error| {
                error!(
                    %offchain_order_id,
                    %symbol,
                    %error,
                    "Failed to load offchain order after Place; cannot determine post-broker state"
                );
            })
            .context("failed to load offchain order after Place")?;
        reconcile_offchain_order_state(
            loaded_order,
            position_store,
            symbol,
            offchain_order_id,
            PlacementContext::PostPlacement,
        )
        .await
    }

    /// Whether the offchain order under reconciliation predates this run's
    /// placement or is the order it just placed, so the persisted failure
    /// reason and the refusal error describe the right one.
    #[derive(Debug, Clone, Copy)]
    enum PlacementContext {
        /// A pending order already recorded on the position, inspected before
        /// a new hedge is placed.
        PrePlacement,
        /// The order this run just placed at the broker.
        PostPlacement,
    }

    /// Classifies a persisted hedge and updates its position claim when reconciliation permits.
    async fn reconcile_offchain_order_state(
        loaded_order: Option<OffchainOrder>,
        position_store: &Store<Position>,
        symbol: &Symbol,
        offchain_order_id: OffchainOrderId,
        context: PlacementContext,
    ) -> Result<HedgeDisposition, OperatorError> {
        match loaded_order {
            Some(OffchainOrder::Failed { error, .. }) => {
                // Broker placement failed: clear pending_offchain_order_id so the
                // position is not permanently stuck and the normal pipeline can
                // retry. No broker terminality classification available here;
                // fail-safe preserves.
                position_store
                    .send(
                        symbol,
                        PositionCommand::FailOffChainOrder {
                            offchain_order_id,
                            error,
                            anchor: AnchorDisposition::Preserve,
                            kind: OffchainOrderFailureKind::Failure,
                        },
                    )
                    .await
                    .context("failed to clear the failed offchain order from the position")?;
                Ok(HedgeDisposition::ClearedForRetry)
            }
            Some(
                OffchainOrder::Submitted { .. }
                | OffchainOrder::PartiallyFilled { .. }
                | OffchainOrder::Cancelling { .. },
            ) => Ok(HedgeDisposition::InFlight),
            None => {
                let error = match context {
                    PlacementContext::PrePlacement => {
                        "Existing pending offchain order missing before placement"
                    }
                    PlacementContext::PostPlacement => "Offchain order missing after Place",
                };
                position_store
                    .send(
                        symbol,
                        PositionCommand::FailOffChainOrder {
                            offchain_order_id,
                            error: error.to_owned(),
                            anchor: AnchorDisposition::Preserve,
                            kind: OffchainOrderFailureKind::Failure,
                        },
                    )
                    .await
                    .context("failed to clear the missing offchain order from the position")?;
                Ok(HedgeDisposition::ClearedForRetry)
            }
            Some(OffchainOrder::Pending { .. }) => match context {
                // A Pending order before placement is the legitimate state the
                // live pipeline leaves when it defers its own placement and holds
                // the order for its own retry (process-tx no longer retains a
                // Pending; ADR 0022). Classify it as a deferral so the gate
                // settles the later fill and reports the deferral instead of a
                // rejection with partially applied accounting; the live pipeline
                // still owns retrying the retained intent once admission permits.
                PlacementContext::PrePlacement => Ok(HedgeDisposition::Deferred),
                // After this run placed the order, a Pending state is not a
                // legitimate resting state: the broker call returned without a
                // terminal or in flight order, so refuse rather than clear.
                PlacementContext::PostPlacement => {
                    Err(RejectionReason::OffchainOrderUnexpectedPostPlacementState {
                        offchain_order_id,
                        symbol: symbol.clone(),
                    }
                    .into())
                }
            },
            Some(order @ (OffchainOrder::Filled { .. } | OffchainOrder::Cancelled { .. })) => {
                reconcile_terminal_offchain_order(&order, position_store, symbol, offchain_order_id)
                    .await
            }
        }
    }

    /// Finalizes a position from a terminal hedge while refusing unpriced retained fills.
    async fn reconcile_terminal_offchain_order(
        order: &OffchainOrder,
        position_store: &Store<Position>,
        symbol: &Symbol,
        offchain_order_id: OffchainOrderId,
    ) -> Result<HedgeDisposition, OperatorError> {
        let Some(finalization) = terminal_position_finalization(order) else {
            // Filled and Cancelled orders always classify; reaching here means an
            // invariant broke, not a state the operator can act on.
            return Err(OperatorError::Operational(anyhow::anyhow!(
                "offchain order {offchain_order_id} for {symbol} is terminal but did not \
                 produce a position finalization; refusing to clear the position claim"
            )));
        };

        let command = match finalization {
            TerminalPositionFinalization::UnpricedFill { shares_filled } => {
                return Err(RejectionReason::OffchainOrderUnpricedFill {
                    offchain_order_id,
                    symbol: symbol.clone(),
                    shares_filled,
                }
                .into());
            }
            finalization => {
                let Some(command) =
                    position_command_for_finalization(finalization, offchain_order_id)
                else {
                    // Only UnpricedFill maps to no command, and that arm returned
                    // above; reaching here means an invariant broke.
                    return Err(OperatorError::Operational(anyhow::anyhow!(
                        "offchain order {offchain_order_id} for {symbol} could not be mapped to a \
                         position finalization command; refusing to clear the position claim"
                    )));
                };
                command
            }
        };

        position_store
            .send(symbol, command)
            .await
            .context("failed to finalize the position claim")?;
        Ok(HedgeDisposition::Finalized)
    }

    #[cfg(test)]
    mod tests {
        use std::sync::Arc;
        use std::time::Duration;

        use alloy::primitives::{Address, B256, TxHash, U256};
        use alloy::providers::{ProviderBuilder, mock::Asserter};
        use async_trait::async_trait;
        use chrono::Utc;
        use tokio::sync::Mutex;

        use st0x_config::{
            ChainAssets, ChainEquityAsset, Ctx, ExecutionThreshold, HedgedChain, HedgingAssets,
            OperationMode, PricingCtx, TradingScheduleConfig, TradingScheduleMode,
        };
        use st0x_event_sorcery::{AggregateError, SendError, StoreBuilder};
        use st0x_evm::Chain;
        use st0x_execution::{
            AlpacaBrokerApiError, BuyingPowerReservationCents, CancellationOutcome, ClientOrderId,
            CounterTradePreflight, CounterTradeReservation, CounterTradeSkipReason, Direction,
            ExecutorOrderId, FractionalShares, LimitOrder, MarketOrder, MockExecutor, Positive,
            SupportedExecutor, Symbol,
        };
        use st0x_registry::SymbolCache;

        use crate::bindings::IRaindexV6::{ClearConfigV2, ClearV3};
        use crate::conductor::job::find_backpressure;
        use crate::conductor::{
            TradeProcessingCqrs, execute_acknowledge_fill, execute_mark_acknowledged,
            process_queued_trade,
        };
        use crate::offchain::order::{
            BrokerOrderPlacement, CancellationReason, CounterTradeOrderKind, ExecutorOrderPlacer,
            OffchainOrder, OffchainOrderCommand, OffchainOrderEvent, OffchainOrderFailureKind,
            OffchainOrderId, OrderPlacementResult, OrderPlacer, PlacementAdmission,
            PlacementProvenance, PollOrderStatus, PollOrderStatusJobQueue, RetainedFill,
            noop_order_placer,
        };
        use crate::onchain::trade::RaindexTradeEvent;
        use crate::onchain_trade::{
            InventoryVenue, OnChainTrade as OnChainTradeCqrs, OnChainTradeCommand, OnChainTradeId,
            OnChainTradeSource,
        };
        use crate::position::{AnchorDisposition, Position, PositionCommand, TradeId};
        use crate::test_utils::{
            OnchainTradeBuilder, TEST_POLL_INTERVAL, get_test_order,
            reserving_counter_trade_preflight, try_positive_shares, try_setup_test_db,
            try_setup_test_pools,
        };
        use crate::trading::onchain::inclusion::EmittedOnChain;
        use crate::trading::onchain::trade_accountant::TradeAccountingError;

        use super::{
            HedgeDisposition, OperatorError, PlacedHedgeDisposition, PlacementContext,
            PreflightReservationMismatch, ProcessTxChainContext, ProcessTxFill, ProcessTxOutcome,
            ProcessTxStores, RejectionReason, ensure_decoded_chain_matches,
            gate_fill_for_placement, preflight_placement, process_found_trade, process_tx,
            reconcile_failed_anchor, reconcile_offchain_order_state, reconcile_post_place_state,
        };

        /// Parses a positive share quantity for process-tx fixtures.
        fn positive_shares(value: &str) -> Positive<FractionalShares> {
            try_positive_shares(value).expect("test shares must be valid and positive")
        }

        /// Creates a migrated database for an isolated process-tx test.
        async fn setup_test_db() -> sqlx::SqlitePool {
            try_setup_test_db()
                .await
                .expect("test database setup must succeed")
        }

        /// Reads back the failure kind the placement path persisted on the
        /// offchain order's terminal `Failed` event. A deferral and a genuine
        /// failure are indistinguishable from the aggregate state alone, so the
        /// durable event is the only place the classification is observable.
        async fn persisted_failure_kind(pool: &sqlx::SqlitePool) -> OffchainOrderFailureKind {
            let (payload,): (String,) = sqlx::query_as(
                "SELECT payload FROM events WHERE event_type = 'OffchainOrderEvent::Failed'",
            )
            .fetch_one(pool)
            .await
            .unwrap();
            let event: OffchainOrderEvent = serde_json::from_str(&payload).unwrap();
            let OffchainOrderEvent::Failed { kind, .. } = event else {
                panic!("a Failed event row must deserialize to Failed, got: {event:?}");
            };
            kind
        }

        /// Returns the valid baseline onchain trade fixture used throughout this module.
        fn onchain_trade_builder() -> OnchainTradeBuilder {
            OnchainTradeBuilder::try_new().expect("default onchain trade fixture must be valid")
        }

        #[test]
        fn process_tx_fill_preserves_the_decoded_trade_summary() {
            let trade = onchain_trade_builder().with_log_index(7).build();

            let fill = ProcessTxFill::from(&trade);

            assert_eq!(fill.tx_hash, trade.tx_hash);
            assert_eq!(fill.log_index, 7);
            assert_eq!(&fill.symbol, trade.symbol());
            assert_eq!(fill.direction, trade.direction);
            assert_eq!(fill.quantity, trade.amount);
            assert_eq!(fill.price.get_inner(), trade.price().get_inner());
        }

        #[tokio::test]
        async fn process_tx_names_selected_chain_when_transaction_is_missing() {
            let pool = setup_test_db().await;
            let mut ctx = create_base_test_ctx();
            let trading_chain = HedgedChain::test().chain(Chain::Ethereum).call();
            ctx.chains.insert_secondary(trading_chain.clone());
            let order_placer = noop_order_placer();
            let stores = stores_for(&pool, &order_placer).await;
            let asserter = Asserter::new();
            asserter.push_success(&serde_json::Value::Null);
            let provider = ProviderBuilder::new().connect_mocked_client(asserter);
            let tx_hash = TxHash::repeat_byte(0x44);

            let report = process_tx(
                tx_hash,
                &ctx,
                &pool,
                ProcessTxChainContext::new(&trading_chain, &provider, &SymbolCache::default()),
                &stores,
                order_placer,
                None,
                None,
            )
            .await
            .unwrap();

            assert!(report.fill.is_none());
            assert!(matches!(
                report.outcome,
                ProcessTxOutcome::TransactionNotFound {
                    tx_hash: missing,
                    chain: Chain::Ethereum,
                } if missing == tx_hash
            ));
        }

        /// Builds the minimal application context required by process-tx tests.
        fn create_base_test_ctx() -> Ctx {
            st0x_config::create_test_ctx_with_order_owner(Address::ZERO)
        }

        /// `OrderPlacer` that reports whether the broker still holds an order
        /// for the queried client id, used to drive `reconcile_failed_anchor`.
        struct AnchorPresencePlacer {
            present: bool,
        }

        #[async_trait]
        impl OrderPlacer for AnchorPresencePlacer {
            async fn place_market_order(
                &self,
                _order: MarketOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                panic!("anchor reconciliation must not place")
            }

            async fn place_limit_order(
                &self,
                _order: LimitOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                panic!("anchor reconciliation must not place")
            }

            async fn cancel_order(
                &self,
                _executor_order_id: &ExecutorOrderId,
            ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>> {
                panic!("anchor reconciliation must not cancel")
            }

            async fn get_order_by_client_order_id(
                &self,
                _client_order_id: &ClientOrderId,
            ) -> Result<Option<BrokerOrderPlacement>, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(self.present.then(|| BrokerOrderPlacement {
                    executor_order_id: ExecutorOrderId::new("existing-anchor-order"),
                    symbol: Symbol::new("AAPL").unwrap(),
                    shares: positive_shares("1"),
                    direction: Direction::Sell,
                    placed_at: Utc::now(),
                    is_extended_hours: Some(false),
                    limit_price: None,
                }))
            }
        }

        /// Seeds a `Position` carrying a preserved failed-order anchor for an
        /// Alpaca executor, the state `reconcile_failed_anchor` reconciles.
        async fn seeded_failed_anchor(
            pool: &sqlx::SqlitePool,
        ) -> (
            Arc<st0x_event_sorcery::Store<Position>>,
            Symbol,
            OffchainOrderId,
        ) {
            let (position_store, _) = StoreBuilder::<Position>::new(pool.clone())
                .build(())
                .await
                .unwrap();
            let symbol = Symbol::new("AAPL").unwrap();
            let trade = onchain_trade_builder().build();
            execute_acknowledge_fill(
                &position_store,
                &trade,
                ExecutionThreshold::whole_share(),
                trade.block_timestamp.unwrap(),
            )
            .await
            .unwrap();
            let anchor = OffchainOrderId::new();
            position_store
                .send(
                    &symbol,
                    PositionCommand::PlaceOffChainOrder {
                        offchain_order_id: anchor,
                        shares: positive_shares("1"),
                        direction: Direction::Sell,
                        executor: SupportedExecutor::AlpacaBrokerApi,
                        threshold: ExecutionThreshold::whole_share(),
                    },
                )
                .await
                .unwrap();
            position_store
                .send(
                    &symbol,
                    PositionCommand::FailOffChainOrder {
                        offchain_order_id: anchor,
                        error: "lost placement response".to_string(),
                        anchor: AnchorDisposition::Preserve,
                        kind: OffchainOrderFailureKind::Failure,
                    },
                )
                .await
                .unwrap();
            (position_store, symbol, anchor)
        }

        /// A broker order still exists for the preserved anchor's client id, so
        /// process-tx must refuse rather than double-submit, and must leave the
        /// anchor in place for the liquidity service to reconcile.
        #[tokio::test]
        async fn reconcile_failed_anchor_refuses_when_the_broker_still_holds_the_order() {
            let pool = setup_test_db().await;
            let (position_store, symbol, anchor) = seeded_failed_anchor(&pool).await;

            let error = reconcile_failed_anchor(
                &position_store,
                &AnchorPresencePlacer { present: true },
                &symbol,
                OffchainOrderId::new(),
                SupportedExecutor::AlpacaBrokerApi,
            )
            .await
            .unwrap_err();

            assert!(
                matches!(
                    &error,
                    OperatorError::Rejected(RejectionReason::FailedAnchorStillAtBroker {
                        anchor: refused_anchor,
                        executor_order_id,
                    }) if *refused_anchor == anchor
                        && executor_order_id.as_ref() == "existing-anchor-order"
                ),
                "a broker-present anchor must be a typed rejection, got: {error}"
            );
            let position = position_store.load(&symbol).await.unwrap().unwrap();
            assert_eq!(
                position.last_failed_offchain_order_id,
                Some(anchor),
                "a broker-present anchor must be preserved, not released"
            );
        }

        /// The broker confirms no order for the anchor's client id, so the
        /// anchor is released and a fresh placement may proceed.
        #[tokio::test]
        async fn reconcile_failed_anchor_releases_when_the_broker_confirms_absence() {
            let pool = setup_test_db().await;
            let (position_store, symbol, _anchor) = seeded_failed_anchor(&pool).await;

            let released = reconcile_failed_anchor(
                &position_store,
                &AnchorPresencePlacer { present: false },
                &symbol,
                OffchainOrderId::new(),
                SupportedExecutor::AlpacaBrokerApi,
            )
            .await
            .unwrap();

            assert_eq!(released, None);
            let position = position_store.load(&symbol).await.unwrap().unwrap();
            assert_eq!(
                position.last_failed_offchain_order_id, None,
                "a confirmed-absent anchor must be released"
            );
        }

        /// End to end proof that the shared `process_found_trade` settles the
        /// fill before it surfaces a failed anchor rejection. With a broker
        /// present anchor the path must acknowledge the fresh fill and drop it
        /// from the position pending acknowledgement set before returning
        /// `FailedAnchorStillAtBroker`: the typed rejection reports the fill as
        /// handled, so it may only be returned once accounting is durable.
        #[tokio::test]
        async fn process_found_trade_settles_the_fill_before_rejecting_a_broker_present_anchor() {
            let pool = setup_test_db().await;
            let (position_store, symbol, anchor) = seeded_failed_anchor(&pool).await;

            let mut ctx = create_base_test_ctx();
            ctx.chains.primary_mut().assets.equities.symbols.insert(
                symbol.clone(),
                ChainEquityAsset {
                    tokenized_equity: Address::ZERO,
                    tokenized_equity_derivative: Address::ZERO,
                    vault_ids: vec![],
                    trading: OperationMode::Enabled,
                    rebalancing: OperationMode::Disabled,
                    wrapped_equity_recovery: OperationMode::Disabled,
                    operational_limit: None,
                    target_share: None,
                },
            );

            let onchain_trade = onchain_trade_builder()
                .with_log_index(5)
                .with_block_number(42)
                .build();
            let trade_id =
                OnChainTradeId::new(Chain::Base, onchain_trade.tx_hash, onchain_trade.log_index);
            let position_trade_id = TradeId {
                chain: onchain_trade.chain,
                tx_hash: onchain_trade.tx_hash,
                log_index: onchain_trade.log_index,
            };

            let order_placer: Arc<dyn OrderPlacer> =
                Arc::new(AnchorPresencePlacer { present: true });
            let error = process_found_trade(
                onchain_trade,
                &ctx,
                &pool,
                &stores_for(&pool, &order_placer).await,
                order_placer,
                None,
                None,
            )
            .await
            .unwrap_err();

            assert!(
                matches!(
                    &error,
                    OperatorError::Rejected(RejectionReason::FailedAnchorStillAtBroker {
                        anchor: refused_anchor,
                        executor_order_id,
                    }) if *refused_anchor == anchor
                        && executor_order_id.as_ref() == "existing-anchor-order"
                ),
                "a broker present anchor must reject with FailedAnchorStillAtBroker, got: {error}"
            );

            let (onchain_trade_store, _) = StoreBuilder::<OnChainTradeCqrs>::new(pool.clone())
                .build(())
                .await
                .unwrap();
            let onchain_state = onchain_trade_store
                .load(&trade_id)
                .await
                .unwrap()
                .expect("the fresh fill must be witnessed before the anchor rejection returns");
            assert!(
                onchain_state.is_acknowledged(),
                "the durable onchain marker must be acknowledged before the anchor rejection returns"
            );

            let position = position_store
                .load(&symbol)
                .await
                .unwrap()
                .expect("the position must exist after fill accounting");
            assert!(
                !position
                    .pending_acknowledged_trade_ids
                    .contains(&position_trade_id),
                "the accounted fill must be settled out of the pending acknowledgement set \
                 before the anchor rejection propagates"
            );
        }

        /// `OrderPlacer` that takes `sever` down before reporting the broker
        /// still holds the anchor's order, so the settle that must precede the
        /// typed rejection hits an unavailable database exactly as an
        /// infrastructure outage mid-request would.
        struct AnchorPresenceSeveringPlacer {
            sever: sqlx::SqlitePool,
        }

        #[async_trait]
        impl OrderPlacer for AnchorPresenceSeveringPlacer {
            async fn place_market_order(
                &self,
                _order: MarketOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                panic!("anchor reconciliation must not place")
            }

            async fn place_limit_order(
                &self,
                _order: LimitOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                panic!("anchor reconciliation must not place")
            }

            async fn cancel_order(
                &self,
                _executor_order_id: &ExecutorOrderId,
            ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>> {
                panic!("anchor reconciliation must not cancel")
            }

            async fn get_order_by_client_order_id(
                &self,
                _client_order_id: &ClientOrderId,
            ) -> Result<Option<BrokerOrderPlacement>, Box<dyn std::error::Error + Send + Sync>>
            {
                self.sever.close().await;
                Ok(Some(BrokerOrderPlacement {
                    executor_order_id: ExecutorOrderId::new("existing-anchor-order"),
                    symbol: Symbol::new("AAPL").unwrap(),
                    shares: positive_shares("1"),
                    direction: Direction::Sell,
                    placed_at: Utc::now(),
                    is_extended_hours: Some(false),
                    limit_price: None,
                }))
            }
        }

        /// A settle failure must not be swallowed by the anchor rejection: the
        /// caller would be told the fill was handled while accounting stayed
        /// unwritten and nothing would retry it. The failure surfaces as an
        /// operational error (a 500 the caller retries) and the fill is left in
        /// the position pending acknowledgement set.
        #[tokio::test]
        async fn process_found_trade_is_operational_when_the_anchor_rejection_cannot_settle() {
            let pool = setup_test_db().await;
            let (position_store, symbol, _anchor) = seeded_failed_anchor(&pool).await;

            let mut ctx = create_base_test_ctx();
            ctx.chains.primary_mut().assets.equities.symbols.insert(
                symbol.clone(),
                ChainEquityAsset {
                    tokenized_equity: Address::ZERO,
                    tokenized_equity_derivative: Address::ZERO,
                    vault_ids: vec![],
                    trading: OperationMode::Enabled,
                    rebalancing: OperationMode::Disabled,
                    wrapped_equity_recovery: OperationMode::Disabled,
                    operational_limit: None,
                    target_share: None,
                },
            );

            let onchain_trade = onchain_trade_builder()
                .with_log_index(5)
                .with_block_number(42)
                .build();
            let position_trade_id = TradeId {
                chain: onchain_trade.chain,
                tx_hash: onchain_trade.tx_hash,
                log_index: onchain_trade.log_index,
            };

            // The OnChainTrade marker lives on its own database so the placer can
            // take it down after the fill is witnessed but before the settle the
            // rejection depends on.
            let marker_pool = setup_test_db().await;
            let order_placer: Arc<dyn OrderPlacer> = Arc::new(AnchorPresenceSeveringPlacer {
                sever: marker_pool.clone(),
            });
            let mut stores = stores_for(&pool, &order_placer).await;
            let (marker_store, _) = StoreBuilder::<OnChainTradeCqrs>::new(marker_pool)
                .build(())
                .await
                .unwrap();
            stores.onchain_trade = marker_store;

            let error = process_found_trade(
                onchain_trade,
                &ctx,
                &pool,
                &stores,
                order_placer,
                None,
                None,
            )
            .await
            .unwrap_err();

            let OperatorError::Operational(_) = &error else {
                panic!(
                    "an unsettled fill must surface as a retryable operational failure, \
                     not a typed rejection, got: {error:?}"
                );
            };

            let position = position_store
                .load(&symbol)
                .await
                .unwrap()
                .expect("the position must exist after fill accounting");
            assert!(
                position
                    .pending_acknowledged_trade_ids
                    .contains(&position_trade_id),
                "an unsettled fill must stay in the pending acknowledgement set for the retry"
            );
        }

        /// `OrderPlacer` returning a configured preflight verdict. With `place`
        /// set it also submits successfully so a full `process_found_trade` can
        /// run through broker placement; otherwise placement panics, isolating
        /// the preflight unit tests. `preflighted` records the order the path
        /// priced, so a test can assert the direction and quantity the hedge
        /// was sized for even when nothing is ever sent to the broker.
        struct PreflightPlacer {
            verdict: CounterTradePreflight,
            place: bool,
            preflighted: Arc<Mutex<Option<MarketOrder>>>,
        }

        #[async_trait]
        impl OrderPlacer for PreflightPlacer {
            async fn place_market_order(
                &self,
                order: MarketOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                assert!(self.place, "preflight test must not place");
                Ok(OrderPlacementResult {
                    executor_order_id: ExecutorOrderId::new("test-broker-order-id"),
                    placed_shares: order.shares,
                    placed_at: Utc::now(),
                    is_extended_hours: false,
                    limit_price: None,
                })
            }

            async fn place_limit_order(
                &self,
                _order: LimitOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                panic!("preflight test must not place a limit order")
            }

            async fn cancel_order(
                &self,
                _executor_order_id: &ExecutorOrderId,
            ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>> {
                panic!("preflight test must not cancel")
            }

            async fn preflight_counter_trade_with_reserved_buying_power(
                &self,
                order: MarketOrder,
                _reserved: BuyingPowerReservationCents,
            ) -> Result<CounterTradePreflight, Box<dyn std::error::Error + Send + Sync>>
            {
                *self.preflighted.lock().await = Some(order);
                Ok(self.verdict.clone())
            }
        }

        /// Insufficient buying power must defer the buy hedge (`None`), the
        /// cash-only safety the shared path restored.
        #[tokio::test]
        async fn preflight_buy_defers_when_buying_power_is_insufficient() {
            let pool = setup_test_db().await;
            let deferred = preflight_placement(
                &pool,
                &PreflightPlacer {
                    verdict: CounterTradePreflight::Skipped(
                        CounterTradeSkipReason::InsufficientBuyingPower {
                            estimated_cost_cents: 5_000,
                            available_buying_power_cents: 100,
                        },
                    ),
                    place: false,
                    preflighted: Arc::default(),
                },
                &Symbol::new("AAPL").unwrap(),
                positive_shares("1"),
                Direction::Buy,
                ClientOrderId::from_uuid(uuid::Uuid::new_v4()),
                SupportedExecutor::AlpacaBrokerApi,
            )
            .await
            .unwrap();
            assert!(deferred.is_none(), "insufficient buying power must defer");
        }

        /// An allowed buy preflight returns the broker-approved (possibly
        /// reduced) share count and the reservation to attach to the order.
        #[tokio::test]
        async fn preflight_buy_returns_the_reduced_shares_and_reservation() {
            let pool = setup_test_db().await;
            let (shares, reservation) = preflight_placement(
                &pool,
                &PreflightPlacer {
                    verdict: CounterTradePreflight::Allowed {
                        reservation: Some(CounterTradeReservation::BuyingPower {
                            required: positive_shares("0.5"),
                            estimated_cost_cents: 5_000,
                            available_buying_power_cents: 100_000,
                        }),
                    },
                    place: false,
                    preflighted: Arc::default(),
                },
                &Symbol::new("AAPL").unwrap(),
                positive_shares("1"),
                Direction::Buy,
                ClientOrderId::from_uuid(uuid::Uuid::new_v4()),
                SupportedExecutor::AlpacaBrokerApi,
            )
            .await
            .unwrap()
            .expect("an allowed preflight must return a placement");
            assert_eq!(shares, positive_shares("0.5"));
            assert_eq!(
                reservation,
                Some(BuyingPowerReservationCents::new(5_000).unwrap())
            );
        }

        /// A buy whose preflight answers with an equity reservation is a
        /// malformed verdict from the type erased placer: the buy was never
        /// priced against cash, so it must fail closed instead of placing on
        /// the requested size.
        #[tokio::test]
        async fn preflight_buy_fails_closed_on_an_equity_reservation() {
            let pool = setup_test_db().await;
            let error = preflight_placement(
                &pool,
                &PreflightPlacer {
                    verdict: CounterTradePreflight::Allowed {
                        reservation: Some(CounterTradeReservation::Equity {
                            symbol: Symbol::new("AAPL").unwrap(),
                            required: positive_shares("1"),
                            available: FractionalShares::new(st0x_float_macro::float!(1)),
                        }),
                    },
                    place: false,
                    preflighted: Arc::default(),
                },
                &Symbol::new("AAPL").unwrap(),
                positive_shares("1"),
                Direction::Buy,
                ClientOrderId::from_uuid(uuid::Uuid::new_v4()),
                SupportedExecutor::AlpacaBrokerApi,
            )
            .await
            .unwrap_err();

            let OperatorError::PreflightReservationMismatch(mismatch) = &error else {
                panic!("a buy reserved against equity must be a typed mismatch, got: {error}");
            };
            assert!(
                matches!(
                    mismatch,
                    PreflightReservationMismatch::BuyReservedEquity { symbol }
                        if symbol == &Symbol::new("AAPL").unwrap()
                ),
                "got: {mismatch}"
            );
        }

        /// A sell whose preflight answers with a buying power reservation never
        /// reserved inventory against the hedge floor, so it must fail closed.
        #[tokio::test]
        async fn preflight_sell_fails_closed_on_a_buying_power_reservation() {
            let pool = setup_test_db().await;
            let error = preflight_placement(
                &pool,
                &PreflightPlacer {
                    verdict: CounterTradePreflight::Allowed {
                        reservation: Some(CounterTradeReservation::BuyingPower {
                            required: positive_shares("1"),
                            estimated_cost_cents: 5_000,
                            available_buying_power_cents: 100_000,
                        }),
                    },
                    place: false,
                    preflighted: Arc::default(),
                },
                &Symbol::new("AAPL").unwrap(),
                positive_shares("1"),
                Direction::Sell,
                ClientOrderId::from_uuid(uuid::Uuid::new_v4()),
                SupportedExecutor::AlpacaBrokerApi,
            )
            .await
            .unwrap_err();

            let OperatorError::PreflightReservationMismatch(mismatch) = &error else {
                panic!("a sell reserved against cash must be a typed mismatch, got: {error}");
            };
            assert!(
                matches!(
                    mismatch,
                    PreflightReservationMismatch::SellReservedBuyingPower { symbol }
                        if symbol == &Symbol::new("AAPL").unwrap()
                ),
                "got: {mismatch}"
            );
        }

        /// Equity reserved in another symbol says nothing about the inventory
        /// this sell would draw down, so the hedge must fail closed rather than
        /// place against a reservation held elsewhere.
        #[tokio::test]
        async fn preflight_sell_fails_closed_on_a_reservation_for_another_symbol() {
            let pool = setup_test_db().await;
            let error = preflight_placement(
                &pool,
                &PreflightPlacer {
                    verdict: CounterTradePreflight::Allowed {
                        reservation: Some(CounterTradeReservation::Equity {
                            symbol: Symbol::new("MSFT").unwrap(),
                            required: positive_shares("1"),
                            available: FractionalShares::new(st0x_float_macro::float!(1)),
                        }),
                    },
                    place: false,
                    preflighted: Arc::default(),
                },
                &Symbol::new("AAPL").unwrap(),
                positive_shares("1"),
                Direction::Sell,
                ClientOrderId::from_uuid(uuid::Uuid::new_v4()),
                SupportedExecutor::AlpacaBrokerApi,
            )
            .await
            .unwrap_err();

            let OperatorError::PreflightReservationMismatch(mismatch) = &error else {
                panic!("equity reserved in another symbol must be a typed mismatch, got: {error}");
            };
            assert!(
                matches!(
                    mismatch,
                    PreflightReservationMismatch::SellReservedOtherSymbol { symbol, reserved }
                        if symbol == &Symbol::new("AAPL").unwrap()
                            && reserved == &Symbol::new("MSFT").unwrap()
                ),
                "got: {mismatch}"
            );
        }

        /// Every Alpaca preflight answers a buy or a sell with a reservation or
        /// a skip, so a reservation-free allow contradicts the executor it ran
        /// for: defense in depth against an `OrderPlacer` implementation that
        /// would otherwise place a hedge nothing reserved cash or inventory
        /// for.
        #[tokio::test]
        async fn preflight_fails_closed_on_an_alpaca_allow_without_a_reservation() {
            let pool = setup_test_db().await;
            for direction in [Direction::Buy, Direction::Sell] {
                let error = preflight_placement(
                    &pool,
                    &PreflightPlacer {
                        verdict: CounterTradePreflight::Allowed { reservation: None },
                        place: false,
                        preflighted: Arc::default(),
                    },
                    &Symbol::new("AAPL").unwrap(),
                    positive_shares("1"),
                    direction,
                    ClientOrderId::from_uuid(uuid::Uuid::new_v4()),
                    SupportedExecutor::AlpacaBrokerApi,
                )
                .await
                .unwrap_err();

                let OperatorError::PreflightReservationMismatch(mismatch) = &error else {
                    panic!("an unreserved Alpaca allow must be a typed mismatch, got: {error}");
                };
                assert!(
                    matches!(
                        mismatch,
                        PreflightReservationMismatch::AllowedWithoutReservation {
                            symbol,
                            direction: refused,
                        } if symbol == &Symbol::new("AAPL").unwrap() && refused == &direction
                    ),
                    "got: {mismatch}"
                );
            }
        }

        /// The dry run executor has no inventory or cash to reserve against, so
        /// its reservation-free allow still places the requested size.
        #[tokio::test]
        async fn preflight_allows_an_unreserved_dry_run_placement() {
            let pool = setup_test_db().await;
            let (shares, reservation) = preflight_placement(
                &pool,
                &PreflightPlacer {
                    verdict: CounterTradePreflight::Allowed { reservation: None },
                    place: false,
                    preflighted: Arc::default(),
                },
                &Symbol::new("AAPL").unwrap(),
                positive_shares("1"),
                Direction::Buy,
                ClientOrderId::from_uuid(uuid::Uuid::new_v4()),
                SupportedExecutor::DryRun,
            )
            .await
            .unwrap()
            .expect("a dry run allow must return a placement");
            assert_eq!(shares, positive_shares("1"));
            assert_eq!(reservation, None);
        }

        /// Insufficient offchain equity inventory must defer the sell hedge
        /// (`None`): the shared preflight now reserves equity for sells too, so
        /// a sell can no longer bypass the available shares check.
        #[tokio::test]
        async fn preflight_sell_defers_when_equity_is_insufficient() {
            let pool = setup_test_db().await;
            let deferred = preflight_placement(
                &pool,
                &PreflightPlacer {
                    verdict: CounterTradePreflight::Skipped(
                        CounterTradeSkipReason::InsufficientEquity {
                            required: positive_shares("1"),
                            available: FractionalShares::new(st0x_float_macro::float!(0)),
                        },
                    ),
                    place: false,
                    preflighted: Arc::default(),
                },
                &Symbol::new("AAPL").unwrap(),
                positive_shares("1"),
                Direction::Sell,
                ClientOrderId::from_uuid(uuid::Uuid::new_v4()),
                SupportedExecutor::AlpacaBrokerApi,
            )
            .await
            .unwrap();
            assert!(
                deferred.is_none(),
                "insufficient equity must defer the sell"
            );
        }

        /// A sell held at the hedge floor because it would drop below the last
        /// non fractionable whole share must defer (`None`).
        #[tokio::test]
        async fn preflight_sell_defers_on_non_fractionable_sizing() {
            let pool = setup_test_db().await;
            let deferred = preflight_placement(
                &pool,
                &PreflightPlacer {
                    verdict: CounterTradePreflight::Skipped(
                        CounterTradeSkipReason::NonFractionableQuantityBelowOne {
                            symbol: Symbol::new("AAPL").unwrap(),
                            requested: positive_shares("0.5"),
                        },
                    ),
                    place: false,
                    preflighted: Arc::default(),
                },
                &Symbol::new("AAPL").unwrap(),
                positive_shares("0.5"),
                Direction::Sell,
                ClientOrderId::from_uuid(uuid::Uuid::new_v4()),
                SupportedExecutor::AlpacaBrokerApi,
            )
            .await
            .unwrap();
            assert!(
                deferred.is_none(),
                "non fractionable sizing below one whole share must defer the sell"
            );
        }

        /// A sell whose equity reservation clamps `required` below the requested
        /// shares must place the clamped quantity, not the raw requested size.
        /// The clamp is the equity inventory safety the buy only gate skipped:
        /// `process_found_trade` must size the sell hedge from the preflight,
        /// not from raw net exposure.
        #[tokio::test]
        async fn process_tx_sell_places_the_floor_clamped_quantity() {
            let pool = setup_test_db().await;

            let mut ctx = create_base_test_ctx();
            ctx.chains.primary_mut().assets.equities.symbols.insert(
                Symbol::new("AAPL").unwrap(),
                ChainEquityAsset {
                    tokenized_equity: Address::ZERO,
                    tokenized_equity_derivative: Address::ZERO,
                    vault_ids: vec![],
                    trading: OperationMode::Enabled,
                    rebalancing: OperationMode::Disabled,
                    wrapped_equity_recovery: OperationMode::Disabled,
                    operational_limit: None,
                    target_share: None,
                },
            );

            // A 2 share buy nets +2, so the hedge is a Sell of 2 requested
            // shares; the equity reservation clamps that to 1.
            let order_placer: Arc<dyn OrderPlacer> = Arc::new(PreflightPlacer {
                verdict: CounterTradePreflight::Allowed {
                    reservation: Some(CounterTradeReservation::Equity {
                        symbol: Symbol::new("AAPL").unwrap(),
                        required: positive_shares("1"),
                        available: FractionalShares::new(st0x_float_macro::float!(1)),
                    }),
                },
                place: true,
                preflighted: Arc::default(),
            });
            let onchain_trade = onchain_trade_builder()
                .with_block_number(42)
                .with_amount(st0x_float_macro::float!(2))
                .build();

            let outcome = process_found_trade(
                onchain_trade,
                &ctx,
                &pool,
                &stores_for(&pool, &order_placer).await,
                order_placer,
                None,
                None,
            )
            .await
            .unwrap();
            let ProcessTxOutcome::HedgePlaced {
                shares, direction, ..
            } = &outcome
            else {
                panic!("a clamped sell must still place a hedge, got: {outcome:?}");
            };
            assert_eq!(
                *direction,
                Direction::Sell,
                "an onchain buy hedges with an offchain sell"
            );
            assert_eq!(
                *shares,
                positive_shares("1"),
                "the placed hedge must carry the floor clamped quantity, not the raw net exposure"
            );

            let (position_store, _) = StoreBuilder::<Position>::new(pool.clone())
                .build(())
                .await
                .unwrap();
            let pending_order_id = position_store
                .load(&Symbol::new("AAPL").unwrap())
                .await
                .unwrap()
                .expect("position must exist after fill accounting")
                .pending_offchain_order_id
                .expect("a submitted hedge must set the pending offchain order id");

            let (offchain_order_store, _) = StoreBuilder::<OffchainOrder>::new(pool.clone())
                .build(noop_order_placer())
                .await
                .unwrap();
            let offchain_order = offchain_order_store
                .load(&pending_order_id)
                .await
                .unwrap()
                .expect("the pending offchain order must be persisted");
            let OffchainOrder::Submitted { shares, .. } = offchain_order else {
                panic!("the clamped sell must submit to the broker, got: {offchain_order:?}");
            };
            assert_eq!(
                shares,
                positive_shares("1"),
                "the persisted broker order must carry the floor clamped quantity"
            );
        }

        /// The process-tx exit path for a deferred hedge, driven end to end: a
        /// sell whose preflight skips must report `PreflightDeferred`, settle
        /// the fill durably (acknowledged in the `OnChainTrade` aggregate and
        /// dropped from the position pending acknowledgement set), and leave no
        /// offchain order claim on the position for the standing pipeline to
        /// trip over.
        #[tokio::test]
        async fn process_tx_sell_deferred_by_preflight_settles_the_fill_without_a_claim() {
            let pool = setup_test_db().await;

            let symbol = Symbol::new("AAPL").unwrap();
            let mut ctx = create_base_test_ctx();
            ctx.chains.primary_mut().assets.equities.symbols.insert(
                symbol.clone(),
                ChainEquityAsset {
                    tokenized_equity: Address::ZERO,
                    tokenized_equity_derivative: Address::ZERO,
                    vault_ids: vec![],
                    trading: OperationMode::Enabled,
                    rebalancing: OperationMode::Disabled,
                    wrapped_equity_recovery: OperationMode::Disabled,
                    operational_limit: None,
                    target_share: None,
                },
            );

            // A 2 share buy nets +2, so the hedge is a Sell of 2 shares that the
            // broker has no inventory to cover.
            let preflighted = Arc::new(Mutex::new(None));
            let order_placer: Arc<dyn OrderPlacer> = Arc::new(PreflightPlacer {
                verdict: CounterTradePreflight::Skipped(
                    CounterTradeSkipReason::InsufficientEquity {
                        required: positive_shares("2"),
                        available: FractionalShares::new(st0x_float_macro::float!(0)),
                    },
                ),
                place: false,
                preflighted: Arc::clone(&preflighted),
            });
            let onchain_trade = onchain_trade_builder()
                .with_block_number(42)
                .with_amount(st0x_float_macro::float!(2))
                .build();
            let trade_id =
                OnChainTradeId::new(Chain::Base, onchain_trade.tx_hash, onchain_trade.log_index);
            let position_trade_id = TradeId {
                chain: onchain_trade.chain,
                tx_hash: onchain_trade.tx_hash,
                log_index: onchain_trade.log_index,
            };

            let outcome = process_found_trade(
                onchain_trade,
                &ctx,
                &pool,
                &stores_for(&pool, &order_placer).await,
                order_placer,
                None,
                None,
            )
            .await
            .unwrap();

            let ProcessTxOutcome::PreflightDeferred {
                symbol: deferred_symbol,
            } = &outcome
            else {
                panic!("a skipped sell preflight must defer the hedge, got: {outcome:?}");
            };
            assert_eq!(deferred_symbol, &symbol);

            let (onchain_trade_store, _) = StoreBuilder::<OnChainTradeCqrs>::new(pool.clone())
                .build(())
                .await
                .unwrap();
            let onchain_state = onchain_trade_store
                .load(&trade_id)
                .await
                .unwrap()
                .expect("the deferred fill must still be witnessed");
            assert!(
                onchain_state.is_acknowledged(),
                "a deferred hedge must leave the fill acknowledged"
            );

            let (position_store, _) = StoreBuilder::<Position>::new(pool.clone())
                .build(())
                .await
                .unwrap();
            let position = position_store
                .load(&symbol)
                .await
                .unwrap()
                .expect("the position must exist after fill accounting");
            assert!(
                !position
                    .pending_acknowledged_trade_ids
                    .contains(&position_trade_id),
                "a deferred hedge must settle the fill out of the pending acknowledgement set"
            );
            assert_eq!(
                position.pending_offchain_order_id, None,
                "a deferred hedge must claim no offchain order"
            );

            let preflighted_order = preflighted
                .lock()
                .await
                .clone()
                .expect("the hedge must be priced by the preflight before it can be deferred");
            assert_eq!(
                preflighted_order.direction,
                Direction::Sell,
                "an onchain buy is preflighted as an offchain sell"
            );
            assert_eq!(
                preflighted_order.shares,
                positive_shares("2"),
                "the deferred sell must be priced for the full net exposure"
            );

            let (offchain_order_events,): (i64,) = sqlx::query_as(
                "SELECT COUNT(*) FROM events WHERE aggregate_type = 'OffchainOrder'",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(
                offchain_order_events, 0,
                "a preflight deferral must never create an offchain order aggregate"
            );
        }

        /// A sell handed a buying power reservation was never checked against
        /// the equity the broker holds, so `process_found_trade` must fail
        /// closed as an operational failure and claim no offchain order rather
        /// than place the raw requested size.
        #[tokio::test]
        async fn process_tx_sell_fails_closed_on_a_buying_power_reservation() {
            let pool = setup_test_db().await;

            let symbol = Symbol::new("AAPL").unwrap();
            let mut ctx = create_base_test_ctx();
            ctx.chains.primary_mut().assets.equities.symbols.insert(
                symbol.clone(),
                ChainEquityAsset {
                    tokenized_equity: Address::ZERO,
                    tokenized_equity_derivative: Address::ZERO,
                    vault_ids: vec![],
                    trading: OperationMode::Enabled,
                    rebalancing: OperationMode::Disabled,
                    wrapped_equity_recovery: OperationMode::Disabled,
                    operational_limit: None,
                    target_share: None,
                },
            );

            let order_placer: Arc<dyn OrderPlacer> = Arc::new(PreflightPlacer {
                verdict: CounterTradePreflight::Allowed {
                    reservation: Some(CounterTradeReservation::BuyingPower {
                        required: positive_shares("2"),
                        estimated_cost_cents: 20_000,
                        available_buying_power_cents: 100_000,
                    }),
                },
                place: false,
                preflighted: Arc::default(),
            });
            let onchain_trade = onchain_trade_builder()
                .with_block_number(42)
                .with_amount(st0x_float_macro::float!(2))
                .build();

            let error = process_found_trade(
                onchain_trade,
                &ctx,
                &pool,
                &stores_for(&pool, &order_placer).await,
                order_placer,
                None,
                None,
            )
            .await
            .unwrap_err();

            let OperatorError::PreflightReservationMismatch(mismatch) = &error else {
                panic!("a mismatched reservation must be a typed preflight mismatch, got: {error}");
            };
            assert!(
                matches!(
                    mismatch,
                    PreflightReservationMismatch::SellReservedBuyingPower { symbol: refused }
                        if refused == &symbol
                ),
                "got: {mismatch}"
            );

            let (position_store, _) = StoreBuilder::<Position>::new(pool.clone())
                .build(())
                .await
                .unwrap();
            let position = position_store
                .load(&symbol)
                .await
                .unwrap()
                .expect("the position must exist after fill accounting");
            assert_eq!(
                position.pending_offchain_order_id, None,
                "a refused hedge must claim no offchain order"
            );
        }

        /// Standalone stores for the offline-path tests; the reactor-wired
        /// path is covered by
        /// `wired_position_store_updates_rebalancing_inventory_immediately`.
        /// The base test context configures no trading schedule, so these
        /// stores come out with the schedule disabled.
        async fn stores_for(
            pool: &sqlx::SqlitePool,
            order_placer: &Arc<dyn OrderPlacer>,
        ) -> ProcessTxStores {
            ProcessTxStores::standalone(pool, &create_base_test_ctx(), order_placer.clone())
                .await
                .expect("standalone stores must build")
        }

        /// `OrderPlacer` that always returns a broker error, used to drive the
        /// `OffchainOrder::Failed` path in `process_found_trade` tests.
        struct FailingOrderPlacer;

        #[async_trait]
        impl OrderPlacer for FailingOrderPlacer {
            async fn place_market_order(
                &self,
                _order: MarketOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                Err("broker rejected the order".into())
            }

            async fn place_limit_order(
                &self,
                _order: LimitOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                Err("broker rejected the order".into())
            }

            async fn cancel_order(
                &self,
                _executor_order_id: &ExecutorOrderId,
            ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>> {
                Err("broker rejected the cancellation".into())
            }

            async fn preflight_counter_trade_with_reserved_buying_power(
                &self,
                order: MarketOrder,
                _reserved: BuyingPowerReservationCents,
            ) -> Result<CounterTradePreflight, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(reserving_counter_trade_preflight(&order))
            }
        }

        /// `OrderPlacer` that always returns a successful placement, used to drive
        /// the `OffchainOrder::Submitted` happy-path in `process_found_trade` tests.
        struct SucceedingOrderPlacer;

        #[async_trait]
        impl OrderPlacer for SucceedingOrderPlacer {
            async fn place_market_order(
                &self,
                order: MarketOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(OrderPlacementResult {
                    executor_order_id: ExecutorOrderId::new("test-broker-order-id"),
                    placed_shares: order.shares,
                    placed_at: Utc::now(),
                    is_extended_hours: false,
                    limit_price: None,
                })
            }

            async fn place_limit_order(
                &self,
                order: LimitOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(OrderPlacementResult {
                    executor_order_id: ExecutorOrderId::new("test-broker-order-id"),
                    placed_shares: order.shares,
                    placed_at: Utc::now(),
                    is_extended_hours: order.extended_hours,
                    limit_price: Some(order.limit_price),
                })
            }

            async fn cancel_order(
                &self,
                _executor_order_id: &ExecutorOrderId,
            ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>> {
                Err("unexpected cancellation from process-tx test order placer".into())
            }

            async fn preflight_counter_trade_with_reserved_buying_power(
                &self,
                order: MarketOrder,
                _reserved: BuyingPowerReservationCents,
            ) -> Result<CounterTradePreflight, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(reserving_counter_trade_preflight(&order))
            }
        }

        /// `process-tx` on an acknowledged fill must fail closed: resolve to the
        /// already-accounted outcome, return immediately, and place NO broker order.
        /// Position-level exposure is the normal pipeline's responsibility.
        #[tokio::test]
        async fn process_tx_skips_accounting_on_acknowledged_fill() {
            let pool = setup_test_db().await;

            // Enable trading so check_execution_readiness would trigger if we fell
            // through -- confirming that the early return fires before hedge placement.
            let mut ctx = create_base_test_ctx();
            ctx.chains.primary_mut().assets.equities.symbols.insert(
                Symbol::new("AAPL").unwrap(),
                ChainEquityAsset {
                    tokenized_equity: Address::ZERO,
                    tokenized_equity_derivative: Address::ZERO,
                    vault_ids: vec![],
                    trading: OperationMode::Enabled,
                    rebalancing: OperationMode::Disabled,
                    wrapped_equity_recovery: OperationMode::Disabled,
                    operational_limit: None,
                    target_share: None,
                },
            );

            let order_placer: Arc<dyn OrderPlacer> = Arc::new(ExecutorOrderPlacer {
                executor: MockExecutor::new(),
                close_flatten_policy: None,
            });

            let source = OnChainTradeSource::Inventory {
                operator: Address::repeat_byte(0x8b),
                venue: InventoryVenue::Bebop,
            };
            let onchain_trade = onchain_trade_builder().with_source(source).build();
            let block_timestamp = onchain_trade.block_timestamp.unwrap();

            let trade_id =
                OnChainTradeId::new(Chain::Base, onchain_trade.tx_hash, onchain_trade.log_index);

            // Pre-seed 1: witness + acknowledge the OnChainTrade aggregate.
            let (onchain_store, _) = StoreBuilder::<OnChainTradeCqrs>::new(pool.clone())
                .build(())
                .await
                .unwrap();

            onchain_store
                .send(
                    &trade_id,
                    OnChainTradeCommand::WitnessAt {
                        source: OnChainTradeSource::Legacy,
                        symbol: onchain_trade.symbol().clone(),
                        amount: onchain_trade.amount.inner(),
                        direction: onchain_trade.direction,
                        price_usdc: onchain_trade.price(),
                        block_number: 1,
                        block_timestamp,
                        filled_at: block_timestamp,
                    },
                )
                .await
                .unwrap();

            onchain_store
                .send(&trade_id, OnChainTradeCommand::Acknowledge)
                .await
                .unwrap();

            // Pre-seed 2: apply the fill to the Position aggregate so there is live
            // unhedged exposure that could trigger hedge placement if we fell through.
            let (pre_position_store, _) = StoreBuilder::<Position>::new(pool.clone())
                .build(())
                .await
                .unwrap();

            execute_acknowledge_fill(
                &pre_position_store,
                &onchain_trade,
                ctx.execution_threshold,
                block_timestamp,
            )
            .await
            .unwrap();

            let outcome = process_found_trade(
                onchain_trade,
                &ctx,
                &pool,
                &stores_for(&pool, &order_placer).await,
                order_placer,
                None,
                None,
            )
            .await
            .unwrap();
            assert!(
                matches!(outcome, ProcessTxOutcome::AlreadyAccounted),
                "acknowledged fill must resolve to AlreadyAccounted, got: {outcome:?}"
            );

            // No second OnChainOrderFilled event: the pre-seeded fill must not be
            // re-counted by the re-run.
            let (fill_count,): (i64,) =
                sqlx::query_as("SELECT COUNT(*) FROM events WHERE event_type = ?")
                    .bind("PositionEvent::OnChainOrderFilled")
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(
                fill_count, 1,
                "re-running process-tx on an acknowledged fill must not emit a second fill event"
            );

            // No OffchainOrder placed: fail-closed must not place a spurious hedge
            // driven by the live position exposure.
            let (order_count,): (i64,) = sqlx::query_as(
                "SELECT COUNT(*) FROM events WHERE event_type LIKE 'OffchainOrderEvent%'",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(
                order_count, 0,
                "re-running process-tx on an acknowledged fill must place no broker order"
            );

            let repaired = onchain_store
                .load(&trade_id)
                .await
                .unwrap()
                .expect("the acknowledged legacy trade must still exist");
            assert_eq!(
                repaired.source(),
                source,
                "process-tx must append chain-backed venue attribution without re-accounting the fill"
            );
            let (attribution_count,): (i64,) =
                sqlx::query_as("SELECT COUNT(*) FROM events WHERE event_type = ?")
                    .bind("OnChainTradeEvent::SourceAttributed")
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(
                attribution_count, 1,
                "source repair must append exactly one CQRS attribution event"
            );
        }

        /// `process-tx` must resume the acknowledge step when the fill was witnessed
        /// but not yet acknowledged (crash-recovery window). After the call, the
        /// `OnChainTrade` record must be acknowledged and exactly one position fill
        /// event must exist. Trading is disabled in this test context so hedge
        /// placement is not reached.
        #[tokio::test]
        async fn process_tx_resumes_witnessed_but_unacknowledged_fill() {
            let pool = setup_test_db().await;
            let ctx = create_base_test_ctx();
            let order_placer: Arc<dyn OrderPlacer> = Arc::new(ExecutorOrderPlacer {
                executor: MockExecutor::new(),
                close_flatten_policy: None,
            });

            let onchain_trade = onchain_trade_builder().with_block_number(1).build();
            let block_timestamp = onchain_trade.block_timestamp.unwrap();

            let trade_id =
                OnChainTradeId::new(Chain::Base, onchain_trade.tx_hash, onchain_trade.log_index);

            // Pre-seed: witness only (not acknowledged -- simulates crash window).
            let (store, _) = StoreBuilder::<OnChainTradeCqrs>::new(pool.clone())
                .build(())
                .await
                .unwrap();

            store
                .send(
                    &trade_id,
                    OnChainTradeCommand::Witness {
                        source: onchain_trade.source,
                        symbol: onchain_trade.symbol().clone(),
                        amount: onchain_trade.amount.inner(),
                        direction: onchain_trade.direction,
                        price_usdc: onchain_trade.price(),
                        block_number: 1,
                        block_timestamp,
                    },
                )
                .await
                .unwrap();

            // Call process_found_trade: must resume and complete the acknowledge step.
            process_found_trade(
                onchain_trade,
                &ctx,
                &pool,
                &stores_for(&pool, &order_placer).await,
                order_placer,
                None,
                None,
            )
            .await
            .unwrap();

            // Verify the trade is now fully acknowledged.
            let state = store
                .load(&trade_id)
                .await
                .unwrap()
                .expect("OnChainTrade record must exist after resume");
            assert!(
                state.is_acknowledged(),
                "Fill must be acknowledged after process_found_trade resumes the crash-recovery path"
            );

            // Exactly one OnChainOrderFilled event must exist -- the fill accounting
            // ran once during resume, not zero times (dropped) or twice (double-counted).
            let (fill_count,): (i64,) =
                sqlx::query_as("SELECT COUNT(*) FROM events WHERE event_type = ?")
                    .bind("PositionEvent::OnChainOrderFilled")
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(
                fill_count, 1,
                "Resume must apply exactly one fill to the position aggregate"
            );
        }

        /// `process-tx` on a genuinely new fill must witness it in the
        /// `OnChainTrade` aggregate, acknowledge it in the `Position` aggregate,
        /// and mark it acknowledged -- leaving exactly one position fill event.
        #[tokio::test]
        async fn process_tx_witnesses_and_acknowledges_new_fill() {
            let pool = setup_test_db().await;
            let ctx = create_base_test_ctx();
            let order_placer: Arc<dyn OrderPlacer> = Arc::new(ExecutorOrderPlacer {
                executor: MockExecutor::new(),
                close_flatten_policy: None,
            });

            // block_number is required for the Witness step on a new fill.
            let onchain_trade = onchain_trade_builder().with_block_number(42).build();

            let trade_id =
                OnChainTradeId::new(Chain::Base, onchain_trade.tx_hash, onchain_trade.log_index);

            process_found_trade(
                onchain_trade,
                &ctx,
                &pool,
                &stores_for(&pool, &order_placer).await,
                order_placer,
                None,
                None,
            )
            .await
            .unwrap();

            // The OnChainTrade aggregate must be acknowledged.
            let (store, _) = StoreBuilder::<OnChainTradeCqrs>::new(pool.clone())
                .build(())
                .await
                .unwrap();
            let state = store
                .load(&trade_id)
                .await
                .unwrap()
                .expect("OnChainTrade record must exist after processing a new fill");
            assert!(
                state.is_acknowledged(),
                "Fill must be acknowledged in the OnChainTrade aggregate"
            );

            // Exactly one OnChainOrderFilled position event must be in the DB.
            let (fill_count,): (i64,) =
                sqlx::query_as("SELECT COUNT(*) FROM events WHERE event_type = ?")
                    .bind("PositionEvent::OnChainOrderFilled")
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(
                fill_count, 1,
                "Exactly one fill must be applied to the position for a new fill"
            );
        }

        /// After process-tx applies a fill via `process_found_trade`, the normal
        /// pipeline re-detecting the same fill (via `process_queued_trade`) must
        /// return `Ok(None)` -- skipping cleanly -- and must NOT emit a second
        /// `OnChainOrderFilled` event. This is the primary double-count guard test.
        #[tokio::test]
        async fn process_tx_then_normal_path_does_not_double_count() {
            let (pool, apalis_pool) = try_setup_test_pools().await.unwrap();
            let ctx = create_base_test_ctx();
            let order_placer: Arc<dyn OrderPlacer> = Arc::new(ExecutorOrderPlacer {
                executor: MockExecutor::new(),
                close_flatten_policy: None,
            });

            let onchain_trade = onchain_trade_builder().with_block_number(42).build();

            // Step 1: process-tx applies the fill.
            process_found_trade(
                onchain_trade.clone(),
                &ctx,
                &pool,
                &stores_for(&pool, &order_placer).await,
                order_placer.clone(),
                None,
                None,
            )
            .await
            .unwrap();

            // Step 2: Construct TradeProcessingCqrs backed by the same pool so the
            // acknowledged OnChainTrade record written by process-tx is visible.
            let (onchain_trade_store, _) = StoreBuilder::<OnChainTradeCqrs>::new(pool.clone())
                .build(())
                .await
                .unwrap();
            let (position_store, position_projection) = StoreBuilder::<Position>::new(pool.clone())
                .build(())
                .await
                .unwrap();
            let (offchain_order_store, _) = StoreBuilder::<OffchainOrder>::new(pool.clone())
                .build(noop_order_placer())
                .await
                .unwrap();

            let cqrs = TradeProcessingCqrs {
                hedging: HedgingAssets::default(),
                pool: pool.clone(),
                onchain_trade: onchain_trade_store,
                position: position_store,
                position_projection,
                offchain_order: offchain_order_store,
                order_placer,
                execution_threshold: ExecutionThreshold::whole_share(),
                counter_trade_submission_lock: Arc::new(Mutex::new(())),
                close_flatten_policy:
                    crate::trading::offchain::close_flatten::CloseFlattenPolicy::from_secs(900)
                        .unwrap(),
                close_flatten_ramp:
                    crate::trading::offchain::close_flatten::CloseFlattenCrossRamp::new(100, 400)
                        .unwrap(),
                poll_status_queue: PollOrderStatusJobQueue::new(&apalis_pool),
                hedge_queue: crate::trading::offchain::hedge::HedgeJobQueue::new(&apalis_pool),
                poll_interval: TEST_POLL_INTERVAL,
                #[cfg(any(test, feature = "test-support"))]
                placement_barrier: None,
            };

            // The trade_event payload is never accessed because process_queued_trade
            // returns Ok(None) immediately at the is_acknowledged() guard, before
            // reaching the witness step that would use block_number.
            let trade_event = EmittedOnChain {
                chain: Chain::Base,
                event: RaindexTradeEvent::ClearV3(Box::new(ClearV3 {
                    sender: alloy::primitives::Address::ZERO,
                    alice: get_test_order(),
                    bob: get_test_order(),
                    clearConfig: ClearConfigV2 {
                        aliceInputIOIndex: U256::ZERO,
                        aliceOutputIOIndex: U256::ZERO,
                        bobInputIOIndex: U256::ZERO,
                        bobOutputIOIndex: U256::ZERO,
                        aliceBountyVaultId: B256::ZERO,
                        bobBountyVaultId: B256::ZERO,
                    },
                })),
                tx_hash: onchain_trade.tx_hash,
                log_index: onchain_trade.log_index,
                block_number: 42,
                block_timestamp: onchain_trade.block_timestamp,
            };

            // Step 3: Normal pipeline re-detects the same fill.
            let result = process_queued_trade(
                &st0x_execution::MockExecutor::new(),
                &trade_event,
                onchain_trade,
                &cqrs,
                &ChainAssets::default(),
                true,
            )
            .await
            .unwrap();

            assert_eq!(
                result, None,
                "Normal pipeline must skip an already-acknowledged fill"
            );

            // Exactly one OnChainOrderFilled position event: process-tx + pipeline
            // together must not double-count.
            let (fill_count,): (i64,) =
                sqlx::query_as("SELECT COUNT(*) FROM events WHERE event_type = ?")
                    .bind("PositionEvent::OnChainOrderFilled")
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(
                fill_count, 1,
                "process-tx then normal pipeline must produce exactly one position fill event, not two"
            );
        }

        /// A new fill with no `block_number` must surface as the typed
        /// `FillMissingBlockNumber` rejection so the operator sees a loud,
        /// classifiable refusal rather than a silent skip or an opaque 500.
        #[tokio::test]
        async fn process_tx_fails_on_missing_block_number() {
            let pool = setup_test_db().await;
            let ctx = create_base_test_ctx();
            let order_placer: Arc<dyn OrderPlacer> = Arc::new(ExecutorOrderPlacer {
                executor: MockExecutor::new(),
                close_flatten_policy: None,
            });

            let onchain_trade = onchain_trade_builder().with_block_number(None).build();

            let error = process_found_trade(
                onchain_trade,
                &ctx,
                &pool,
                &stores_for(&pool, &order_placer).await,
                order_placer,
                None,
                None,
            )
            .await
            .unwrap_err();

            assert!(
                matches!(
                    error,
                    OperatorError::Rejected(RejectionReason::FillMissingBlockNumber { .. })
                ),
                "a fill with no block_number must be a typed rejection, got: {error}"
            );
        }

        /// A new fill with no `block_timestamp` must return an error so the operator
        /// sees a loud failure rather than a silent skip.
        #[tokio::test]
        async fn process_tx_fails_on_missing_block_timestamp() {
            let pool = setup_test_db().await;
            let ctx = create_base_test_ctx();
            let order_placer: Arc<dyn OrderPlacer> = Arc::new(ExecutorOrderPlacer {
                executor: MockExecutor::new(),
                close_flatten_policy: None,
            });

            // block_number is set so the new-fill branch is reached; block_timestamp
            // is None so the bail fires before the witness step.
            let onchain_trade = onchain_trade_builder()
                .with_block_number(42)
                .with_block_timestamp(None)
                .build();
            let expected_trade_id =
                OnChainTradeId::new(Chain::Base, onchain_trade.tx_hash, onchain_trade.log_index);

            let error = process_found_trade(
                onchain_trade,
                &ctx,
                &pool,
                &stores_for(&pool, &order_placer).await,
                order_placer,
                None,
                None,
            )
            .await
            .unwrap_err();

            let OperatorError::Operational(inner) = &error else {
                panic!(
                    "missing block_timestamp is an accounting failure and must stay \
                     operational, got: {error}"
                );
            };
            let trade_accounting_error = inner
                .downcast_ref::<TradeAccountingError>()
                .expect("missing block_timestamp should bubble up as TradeAccountingError");
            assert!(
                matches!(
                    trade_accounting_error,
                    TradeAccountingError::MissingBlockTimestamp { trade_id }
                        if trade_id == &expected_trade_id
                ),
                "missing block_timestamp must produce \
                 TradeAccountingError::MissingBlockTimestamp for {expected_trade_id}, \
                 got: {trade_accounting_error}"
            );
        }

        /// When broker placement fails (resulting in `OffchainOrder::Failed`),
        /// `process_found_trade` must send `PositionCommand::FailOffChainOrder` to
        /// clear `pending_offchain_order_id`, leaving the position unpending so
        /// the normal pipeline can re-hedge on its next cycle.
        #[tokio::test]
        async fn process_tx_clears_pending_order_on_failed_placement() {
            let pool = setup_test_db().await;

            // Enable trading so check_execution_readiness can trigger hedge placement.
            let mut ctx = create_base_test_ctx();
            ctx.chains.primary_mut().assets.equities.symbols.insert(
                Symbol::new("AAPL").unwrap(),
                ChainEquityAsset {
                    tokenized_equity: Address::ZERO,
                    tokenized_equity_derivative: Address::ZERO,
                    vault_ids: vec![],
                    trading: OperationMode::Enabled,
                    rebalancing: OperationMode::Disabled,
                    wrapped_equity_recovery: OperationMode::Disabled,
                    operational_limit: None,
                    target_share: None,
                },
            );

            let order_placer: Arc<dyn OrderPlacer> = Arc::new(FailingOrderPlacer);
            let onchain_trade = onchain_trade_builder().with_block_number(42).build();

            let outcome = process_found_trade(
                onchain_trade,
                &ctx,
                &pool,
                &stores_for(&pool, &order_placer).await,
                order_placer,
                None,
                None,
            )
            .await
            .unwrap();
            let ProcessTxOutcome::HedgePlacementCleared { symbol } = &outcome else {
                panic!(
                    "failed placement must clear the hedge, not report it placed, got: {outcome:?}"
                );
            };
            assert_eq!(
                symbol,
                &Symbol::new("AAPL").unwrap(),
                "the cleared outcome must name the fill's symbol"
            );

            // pending_offchain_order_id must be cleared: the position must not be
            // permanently stuck after a failed broker placement.
            let (position_store, _) = StoreBuilder::<Position>::new(pool.clone())
                .build(())
                .await
                .unwrap();

            let position = position_store
                .load(&Symbol::new("AAPL").unwrap())
                .await
                .unwrap()
                .expect("Position must exist after fill accounting");

            assert!(
                position.pending_offchain_order_id.is_none(),
                "pending_offchain_order_id must be cleared after a failed broker placement"
            );
        }

        /// A resolved pending placement failure must allow process-tx to continue.
        #[tokio::test]
        async fn existing_pending_cleanup_reports_process_tx_continues_this_run() {
            let pool = setup_test_db().await;
            let symbol = Symbol::new("AAPL").unwrap();
            let offchain_order_id = OffchainOrderId::new();
            let block_timestamp = Utc::now();

            let (position_store, _) = StoreBuilder::<Position>::new(pool.clone())
                .build(())
                .await
                .unwrap();
            let onchain_trade = onchain_trade_builder()
                .with_block_number(42)
                .with_block_timestamp(Some(block_timestamp))
                .build();

            execute_acknowledge_fill(
                &position_store,
                &onchain_trade,
                ExecutionThreshold::whole_share(),
                block_timestamp,
            )
            .await
            .unwrap();

            position_store
                .send(
                    &symbol,
                    PositionCommand::PlaceOffChainOrder {
                        offchain_order_id,
                        shares: positive_shares("1"),
                        direction: Direction::Sell,
                        executor: SupportedExecutor::DryRun,
                        threshold: ExecutionThreshold::whole_share(),
                    },
                )
                .await
                .unwrap();

            let failed_order = OffchainOrder::Failed {
                symbol: symbol.clone(),
                shares: positive_shares("1"),
                requested_shares: None,
                direction: Direction::Sell,
                executor: SupportedExecutor::DryRun,
                retained_fill: None,
                filled_shares: None,
                executor_order_id: None,
                error: "previous placement failed".to_string(),
                placed_at: block_timestamp,
                failed_at: block_timestamp,
                market_session: st0x_execution::MarketSession::Regular,
                close_flatten: false,
                kind: OffchainOrderFailureKind::Failure,
            };

            let disposition = reconcile_offchain_order_state(
                Some(failed_order),
                &position_store,
                &symbol,
                offchain_order_id,
                PlacementContext::PrePlacement,
            )
            .await
            .unwrap();
            assert!(
                matches!(disposition, HedgeDisposition::ClearedForRetry),
                "a fresh placement failure must clear the pending marker for retry, got: {disposition:?}"
            );

            let position = position_store
                .load(&symbol)
                .await
                .unwrap()
                .expect("position should exist");
            assert_eq!(
                position.last_failed_offchain_order_id,
                Some(offchain_order_id),
                "a fresh placement failure with no broker order id must preserve \
                 the anchor"
            );
        }

        /// Post-placement failures with broker IDs must preserve the retry anchor.
        #[tokio::test]
        async fn reconcile_loaded_post_place_state_failed_with_executor_id_preserves_the_anchor() {
            let pool = setup_test_db().await;
            let symbol = Symbol::new("AAPL").unwrap();
            let offchain_order_id = OffchainOrderId::new();
            let block_timestamp = Utc::now();

            let (position_store, _) = StoreBuilder::<Position>::new(pool.clone())
                .build(())
                .await
                .unwrap();
            let onchain_trade = onchain_trade_builder()
                .with_block_number(42)
                .with_block_timestamp(Some(block_timestamp))
                .build();

            execute_acknowledge_fill(
                &position_store,
                &onchain_trade,
                ExecutionThreshold::whole_share(),
                block_timestamp,
            )
            .await
            .unwrap();

            position_store
                .send(
                    &symbol,
                    PositionCommand::PlaceOffChainOrder {
                        offchain_order_id,
                        shares: positive_shares("1"),
                        direction: Direction::Sell,
                        executor: SupportedExecutor::DryRun,
                        threshold: ExecutionThreshold::whole_share(),
                    },
                )
                .await
                .unwrap();

            let failed_order = OffchainOrder::Failed {
                symbol: symbol.clone(),
                shares: positive_shares("1"),
                requested_shares: None,
                direction: Direction::Sell,
                executor: SupportedExecutor::DryRun,
                retained_fill: None,
                filled_shares: None,
                executor_order_id: Some(ExecutorOrderId::new("already-poll-failed")),
                error: "previous placement failed".to_string(),
                placed_at: block_timestamp,
                failed_at: block_timestamp,
                market_session: st0x_execution::MarketSession::Regular,
                close_flatten: false,
                kind: OffchainOrderFailureKind::Failure,
            };

            let disposition = reconcile_offchain_order_state(
                Some(failed_order),
                &position_store,
                &symbol,
                offchain_order_id,
                PlacementContext::PostPlacement,
            )
            .await
            .unwrap();
            assert!(
                matches!(disposition, HedgeDisposition::ClearedForRetry),
                "a placement failure must clear the pending marker for retry, got: {disposition:?}"
            );

            let position = position_store
                .load(&symbol)
                .await
                .unwrap()
                .expect("position should exist");
            assert_eq!(
                position.last_failed_offchain_order_id,
                Some(offchain_order_id),
                "this path has no broker-terminality classification to derive \
                 from, so a broker order id alone must not release the anchor"
            );
        }

        /// Missing post-placement order state must clear the pending position claim.
        #[tokio::test]
        async fn process_tx_clears_pending_order_when_post_place_order_missing() {
            let pool = setup_test_db().await;
            let symbol = Symbol::new("AAPL").unwrap();
            let offchain_order_id = OffchainOrderId::new();
            let onchain_trade = onchain_trade_builder().with_block_number(42).build();
            let block_timestamp = onchain_trade
                .block_timestamp
                .expect("test trade should have a block timestamp");

            let (position_store, _) = StoreBuilder::<Position>::new(pool.clone())
                .build(())
                .await
                .unwrap();
            let (offchain_order_store, _) = StoreBuilder::<OffchainOrder>::new(pool.clone())
                .build(noop_order_placer())
                .await
                .unwrap();

            execute_acknowledge_fill(
                &position_store,
                &onchain_trade,
                ExecutionThreshold::whole_share(),
                block_timestamp,
            )
            .await
            .unwrap();

            position_store
                .send(
                    &symbol,
                    PositionCommand::PlaceOffChainOrder {
                        offchain_order_id,
                        shares: positive_shares("1"),
                        direction: Direction::Sell,
                        executor: SupportedExecutor::DryRun,
                        threshold: ExecutionThreshold::whole_share(),
                    },
                )
                .await
                .unwrap();

            let disposition = reconcile_post_place_state(
                &offchain_order_store,
                &position_store,
                &symbol,
                offchain_order_id,
            )
            .await
            .unwrap();
            assert!(
                matches!(disposition, HedgeDisposition::ClearedForRetry),
                "missing OffchainOrder state must clear the pending marker for retry, got: {disposition:?}"
            );

            let position = position_store
                .load(&symbol)
                .await
                .unwrap()
                .expect("position should exist after setup");
            assert_eq!(
                position.pending_offchain_order_id, None,
                "missing OffchainOrder state must clear the position's pending id"
            );
        }

        /// A cancelling hedge must remain in flight until cancellation is terminal.
        #[tokio::test]
        async fn existing_cancelling_pending_order_remains_in_flight() {
            let pool = setup_test_db().await;
            let symbol = Symbol::new("AAPL").unwrap();
            let offchain_order_id = OffchainOrderId::new();
            let onchain_trade = onchain_trade_builder().with_block_number(42).build();
            let block_timestamp = onchain_trade
                .block_timestamp
                .expect("test trade should have a block timestamp");

            let (position_store, _) = StoreBuilder::<Position>::new(pool.clone())
                .build(())
                .await
                .unwrap();

            execute_acknowledge_fill(
                &position_store,
                &onchain_trade,
                ExecutionThreshold::whole_share(),
                block_timestamp,
            )
            .await
            .unwrap();

            position_store
                .send(
                    &symbol,
                    PositionCommand::PlaceOffChainOrder {
                        offchain_order_id,
                        shares: positive_shares("1"),
                        direction: Direction::Sell,
                        executor: SupportedExecutor::DryRun,
                        threshold: ExecutionThreshold::whole_share(),
                    },
                )
                .await
                .unwrap();

            let cancelling_order = OffchainOrder::Cancelling {
                symbol: symbol.clone(),
                shares: positive_shares("1"),
                requested_shares: None,
                retained_fill: None,
                direction: Direction::Sell,
                executor: SupportedExecutor::DryRun,
                executor_order_id: ExecutorOrderId::new("broker-order-id"),
                reason: CancellationReason::MarketOpenReplacement,
                placed_at: block_timestamp,
                submitted_at: block_timestamp,
                cancel_requested_at: block_timestamp,
                market_session: st0x_execution::MarketSession::Regular,
                close_flatten: false,
            };

            let outcome = reconcile_offchain_order_state(
                Some(cancelling_order),
                &position_store,
                &symbol,
                offchain_order_id,
                PlacementContext::PrePlacement,
            )
            .await
            .unwrap();

            assert!(
                matches!(outcome, HedgeDisposition::InFlight),
                "cancelling orders must remain in flight until broker cancellation confirms"
            );

            let position = position_store
                .load(&symbol)
                .await
                .unwrap()
                .expect("position should exist after setup");
            assert_eq!(
                position.pending_offchain_order_id,
                Some(offchain_order_id),
                "Cancelling must leave the position claim in place"
            );
        }

        /// A cancelled hedge without a retained fill must clear its position claim.
        #[tokio::test]
        async fn existing_cancelled_pending_order_clears_position_claim() {
            let pool = setup_test_db().await;
            let symbol = Symbol::new("AAPL").unwrap();
            let offchain_order_id = OffchainOrderId::new();
            let onchain_trade = onchain_trade_builder().with_block_number(42).build();
            let block_timestamp = onchain_trade
                .block_timestamp
                .expect("test trade should have a block timestamp");

            let (position_store, _) = StoreBuilder::<Position>::new(pool.clone())
                .build(())
                .await
                .unwrap();

            execute_acknowledge_fill(
                &position_store,
                &onchain_trade,
                ExecutionThreshold::whole_share(),
                block_timestamp,
            )
            .await
            .unwrap();

            position_store
                .send(
                    &symbol,
                    PositionCommand::PlaceOffChainOrder {
                        offchain_order_id,
                        shares: positive_shares("1"),
                        direction: Direction::Sell,
                        executor: SupportedExecutor::DryRun,
                        threshold: ExecutionThreshold::whole_share(),
                    },
                )
                .await
                .unwrap();

            let cancelled_order = OffchainOrder::Cancelled {
                symbol: symbol.clone(),
                shares: positive_shares("1"),
                requested_shares: Some(positive_shares("1")),
                retained_fill: None,
                filled_shares: Some(FractionalShares::ZERO),
                direction: Direction::Sell,
                executor: SupportedExecutor::DryRun,
                executor_order_id: ExecutorOrderId::new("broker-order-id"),
                reason: CancellationReason::MarketOpenReplacement,
                placed_at: block_timestamp,
                cancelled_at: block_timestamp,
            };

            let outcome = reconcile_offchain_order_state(
                Some(cancelled_order),
                &position_store,
                &symbol,
                offchain_order_id,
                PlacementContext::PrePlacement,
            )
            .await
            .unwrap();

            assert!(
                matches!(outcome, HedgeDisposition::Finalized),
                "cancelled orders with no retained fill must finalize and clear the pending claim"
            );

            let position = position_store
                .load(&symbol)
                .await
                .unwrap()
                .expect("position should exist after setup");
            assert_eq!(
                position.pending_offchain_order_id, None,
                "Cancelled must clear the position claim through CancelOffChainOrder"
            );
        }

        /// Seeds a position that holds `offchain_order_id` as its pending hedge,
        /// the state both a stale pointer and a fresh placement leave behind.
        async fn seed_position_with_pending_order(
            pool: &sqlx::SqlitePool,
            symbol: &Symbol,
            offchain_order_id: OffchainOrderId,
            block_timestamp: chrono::DateTime<chrono::Utc>,
        ) -> Arc<st0x_event_sorcery::Store<Position>> {
            let (position_store, _) = StoreBuilder::<Position>::new(pool.clone())
                .build(())
                .await
                .unwrap();
            let onchain_trade = onchain_trade_builder()
                .with_block_number(42)
                .with_block_timestamp(Some(block_timestamp))
                .build();
            execute_acknowledge_fill(
                &position_store,
                &onchain_trade,
                ExecutionThreshold::whole_share(),
                block_timestamp,
            )
            .await
            .unwrap();
            position_store
                .send(
                    symbol,
                    PositionCommand::PlaceOffChainOrder {
                        offchain_order_id,
                        shares: positive_shares("1"),
                        direction: Direction::Sell,
                        executor: SupportedExecutor::DryRun,
                        threshold: ExecutionThreshold::whole_share(),
                    },
                )
                .await
                .unwrap();
            position_store
        }

        /// Missing-order audit events must identify whether placement already ran.
        #[tokio::test]
        async fn missing_pending_order_audit_reason_names_the_placement_phase() {
            for (context, expected_reason) in [
                (
                    PlacementContext::PrePlacement,
                    "Existing pending offchain order missing before placement",
                ),
                (
                    PlacementContext::PostPlacement,
                    "Offchain order missing after Place",
                ),
            ] {
                let pool = setup_test_db().await;
                let symbol = Symbol::new("AAPL").unwrap();
                let offchain_order_id = OffchainOrderId::new();
                let position_store =
                    seed_position_with_pending_order(&pool, &symbol, offchain_order_id, Utc::now())
                        .await;

                let disposition = reconcile_offchain_order_state(
                    None,
                    &position_store,
                    &symbol,
                    offchain_order_id,
                    context,
                )
                .await
                .unwrap();
                assert!(
                    matches!(disposition, HedgeDisposition::ClearedForRetry),
                    "a missing order must clear the pending marker for retry under {context:?}, got: {disposition:?}"
                );

                let (reason,): (String,) = sqlx::query_as(
                    "SELECT json_extract(payload, '$.OffChainOrderFailed.error') FROM events \
                     WHERE event_type = 'PositionEvent::OffChainOrderFailed'",
                )
                .fetch_one(&pool)
                .await
                .unwrap();
                assert_eq!(
                    reason, expected_reason,
                    "the persisted audit reason must name the placement phase for {context:?}"
                );
            }
        }

        /// A Pending order after placement is not a legitimate resting state and
        /// must surface as a typed rejection that keeps the position claim.
        #[tokio::test]
        async fn pending_order_after_placement_is_a_typed_rejection() {
            let pool = setup_test_db().await;
            let symbol = Symbol::new("AAPL").unwrap();
            let offchain_order_id = OffchainOrderId::new();
            let block_timestamp = Utc::now();
            let position_store = seed_position_with_pending_order(
                &pool,
                &symbol,
                offchain_order_id,
                block_timestamp,
            )
            .await;
            let pending_order = OffchainOrder::Pending {
                symbol: symbol.clone(),
                shares: positive_shares("1"),
                direction: Direction::Sell,
                executor: SupportedExecutor::DryRun,
                placed_at: block_timestamp,
                client_order_id: None,
                limit_price: None,
                market_session: st0x_execution::MarketSession::Regular,
                close_flatten: false,
                buying_power_reservation: None,
                provenance: PlacementProvenance::LivePipeline,
            };

            let error = reconcile_offchain_order_state(
                Some(pending_order),
                &position_store,
                &symbol,
                offchain_order_id,
                PlacementContext::PostPlacement,
            )
            .await
            .unwrap_err();
            assert!(
                matches!(
                    &error,
                    OperatorError::Rejected(
                        RejectionReason::OffchainOrderUnexpectedPostPlacementState {
                            offchain_order_id: id,
                            symbol: rejected,
                        },
                    ) if *id == offchain_order_id && *rejected == symbol
                ),
                "a Pending order after placement must be a typed rejection carrying the order, got: {error}"
            );

            let position = position_store
                .load(&symbol)
                .await
                .unwrap()
                .expect("position should exist after setup");
            assert_eq!(
                position.pending_offchain_order_id,
                Some(offchain_order_id),
                "a refusal must leave the position claim in place"
            );
        }

        /// A Pending order before placement is the state the live pipeline leaves
        /// when it defers its own placement and holds the order for its own retry;
        /// it must classify as a deferral and keep the claim so the later fill can
        /// settle instead of surfacing a rejection or a second hedge over it.
        #[tokio::test]
        async fn retained_pending_order_before_placement_is_a_deferral() {
            let pool = setup_test_db().await;
            let symbol = Symbol::new("AAPL").unwrap();
            let offchain_order_id = OffchainOrderId::new();
            let block_timestamp = Utc::now();
            let position_store = seed_position_with_pending_order(
                &pool,
                &symbol,
                offchain_order_id,
                block_timestamp,
            )
            .await;
            let pending_order = OffchainOrder::Pending {
                symbol: symbol.clone(),
                shares: positive_shares("1"),
                direction: Direction::Sell,
                executor: SupportedExecutor::DryRun,
                placed_at: block_timestamp,
                client_order_id: None,
                limit_price: None,
                market_session: st0x_execution::MarketSession::Regular,
                close_flatten: false,
                buying_power_reservation: None,
                provenance: PlacementProvenance::LivePipeline,
            };

            let disposition = reconcile_offchain_order_state(
                Some(pending_order),
                &position_store,
                &symbol,
                offchain_order_id,
                PlacementContext::PrePlacement,
            )
            .await
            .unwrap();
            assert!(
                matches!(disposition, HedgeDisposition::Deferred),
                "a retained Pending order before placement must classify as a deferral, got: {disposition:?}"
            );

            let position = position_store
                .load(&symbol)
                .await
                .unwrap()
                .expect("position should exist after setup");
            assert_eq!(
                position.pending_offchain_order_id,
                Some(offchain_order_id),
                "a deferral must leave the position claim in place"
            );
        }

        /// An unpriced terminal fill must reject reconciliation and retain the claim.
        #[tokio::test]
        async fn unpriced_terminal_fill_is_a_typed_rejection_and_keeps_the_claim() {
            let pool = setup_test_db().await;
            let symbol = Symbol::new("AAPL").unwrap();
            let offchain_order_id = OffchainOrderId::new();
            let block_timestamp = Utc::now();
            let position_store = seed_position_with_pending_order(
                &pool,
                &symbol,
                offchain_order_id,
                block_timestamp,
            )
            .await;
            let shares_filled: FractionalShares = "0.5".parse().unwrap();
            let cancelled_order = OffchainOrder::Cancelled {
                symbol: symbol.clone(),
                shares: positive_shares("1"),
                requested_shares: Some(positive_shares("1")),
                retained_fill: Some(RetainedFill::Unpriced { shares_filled }),
                filled_shares: Some(shares_filled),
                direction: Direction::Sell,
                executor: SupportedExecutor::DryRun,
                executor_order_id: ExecutorOrderId::new("broker-order-id"),
                reason: CancellationReason::MarketOpenReplacement,
                placed_at: block_timestamp,
                cancelled_at: block_timestamp,
            };

            let error = reconcile_offchain_order_state(
                Some(cancelled_order),
                &position_store,
                &symbol,
                offchain_order_id,
                PlacementContext::PrePlacement,
            )
            .await
            .unwrap_err();
            assert!(
                matches!(
                    &error,
                    OperatorError::Rejected(RejectionReason::OffchainOrderUnpricedFill {
                        offchain_order_id: id,
                        symbol: rejected,
                        ..
                    }) if *id == offchain_order_id && *rejected == symbol
                ),
                "an unpriced terminal fill must be a typed rejection carrying the order, got: {error}"
            );

            let position = position_store
                .load(&symbol)
                .await
                .unwrap()
                .expect("position should exist after setup");
            assert_eq!(
                position.pending_offchain_order_id,
                Some(offchain_order_id),
                "an unpriced fill must leave the position claim in place"
            );
        }

        /// When a second client shares the same pool and witnesses the fill first,
        /// `process_found_trade` must resume the acknowledge step and complete it --
        /// not silently drop the fill. The fill must be accounted exactly once.
        ///
        /// This covers the concurrent-witnessed sub-path: the outer load sees
        /// `Some(Witnessed)` from the peer writer's record and resumes.
        #[tokio::test]
        async fn process_tx_concurrent_witness_resumes_acknowledge() {
            let pool = setup_test_db().await;
            let ctx = create_base_test_ctx();
            let order_placer: Arc<dyn OrderPlacer> = Arc::new(ExecutorOrderPlacer {
                executor: MockExecutor::new(),
                close_flatten_policy: None,
            });

            let onchain_trade = onchain_trade_builder().with_block_number(42).build();
            let block_timestamp = onchain_trade.block_timestamp.unwrap();

            let trade_id =
                OnChainTradeId::new(Chain::Base, onchain_trade.tx_hash, onchain_trade.log_index);

            // Simulate a concurrent writer (e.g. the normal pipeline) that witnesses
            // the fill via its own store instance backed by the same pool. When
            // process_found_trade's internal store loads the aggregate, it will see
            // Some(Witnessed) and resume rather than re-witness.
            let (store_a, _) = StoreBuilder::<OnChainTradeCqrs>::new(pool.clone())
                .build(())
                .await
                .unwrap();

            store_a
                .send(
                    &trade_id,
                    OnChainTradeCommand::Witness {
                        source: onchain_trade.source,
                        symbol: onchain_trade.symbol().clone(),
                        amount: onchain_trade.amount.inner(),
                        direction: onchain_trade.direction,
                        price_usdc: onchain_trade.price(),
                        block_number: 42,
                        block_timestamp,
                    },
                )
                .await
                .unwrap();

            // Call process_found_trade: must resume the acknowledge step and complete
            // it rather than silently dropping the fill.
            process_found_trade(
                onchain_trade,
                &ctx,
                &pool,
                &stores_for(&pool, &order_placer).await,
                order_placer,
                None,
                None,
            )
            .await
            .unwrap();

            // The trade must be fully acknowledged after the resume.
            let state = store_a
                .load(&trade_id)
                .await
                .unwrap()
                .expect("OnChainTrade record must exist after concurrent resume");
            assert!(
                state.is_acknowledged(),
                "Fill must be acknowledged after process_found_trade resumes from concurrent witness"
            );

            // Exactly one fill event: the concurrent write-then-resume must not
            // double-count or drop the fill.
            let (fill_count,): (i64,) =
                sqlx::query_as("SELECT COUNT(*) FROM events WHERE event_type = ?")
                    .bind("PositionEvent::OnChainOrderFilled")
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(
                fill_count, 1,
                "Concurrent witness + resume must apply exactly one fill to the position"
            );
        }

        /// When a concurrent writer has already fully acknowledged the fill,
        /// `process_found_trade` must fail closed: return early without placing a
        /// hedge or emitting a second fill event.
        ///
        /// This covers the concurrent-acknowledged sub-path: the outer load sees
        /// `Some(Acknowledged)` from the peer writer's record and exits immediately.
        #[tokio::test]
        async fn process_tx_concurrent_acknowledged_fails_closed() {
            let pool = setup_test_db().await;

            // Enable trading so that if we fell through to hedge placement we would
            // know -- confirming the early return fires before any of that.
            let mut ctx = create_base_test_ctx();
            ctx.chains.primary_mut().assets.equities.symbols.insert(
                Symbol::new("AAPL").unwrap(),
                ChainEquityAsset {
                    tokenized_equity: Address::ZERO,
                    tokenized_equity_derivative: Address::ZERO,
                    vault_ids: vec![],
                    trading: OperationMode::Enabled,
                    rebalancing: OperationMode::Disabled,
                    wrapped_equity_recovery: OperationMode::Disabled,
                    operational_limit: None,
                    target_share: None,
                },
            );

            let order_placer: Arc<dyn OrderPlacer> = Arc::new(ExecutorOrderPlacer {
                executor: MockExecutor::new(),
                close_flatten_policy: None,
            });

            let onchain_trade = onchain_trade_builder().with_block_number(42).build();
            let block_timestamp = onchain_trade.block_timestamp.unwrap();

            let trade_id =
                OnChainTradeId::new(Chain::Base, onchain_trade.tx_hash, onchain_trade.log_index);

            // Simulate a concurrent writer that has fully processed the fill.
            let (store_a, _) = StoreBuilder::<OnChainTradeCqrs>::new(pool.clone())
                .build(())
                .await
                .unwrap();

            store_a
                .send(
                    &trade_id,
                    OnChainTradeCommand::Witness {
                        source: onchain_trade.source,
                        symbol: onchain_trade.symbol().clone(),
                        amount: onchain_trade.amount.inner(),
                        direction: onchain_trade.direction,
                        price_usdc: onchain_trade.price(),
                        block_number: 42,
                        block_timestamp,
                    },
                )
                .await
                .unwrap();

            store_a
                .send(&trade_id, OnChainTradeCommand::Acknowledge)
                .await
                .unwrap();

            // Apply the fill to the position so there is live unhedged exposure that
            // could trigger hedge placement if we fell through.
            let (pre_position_store, _) = StoreBuilder::<Position>::new(pool.clone())
                .build(())
                .await
                .unwrap();

            execute_acknowledge_fill(
                &pre_position_store,
                &onchain_trade,
                ctx.execution_threshold,
                block_timestamp,
            )
            .await
            .unwrap();

            let outcome = process_found_trade(
                onchain_trade,
                &ctx,
                &pool,
                &stores_for(&pool, &order_placer).await,
                order_placer,
                None,
                None,
            )
            .await
            .unwrap();
            assert!(
                matches!(outcome, ProcessTxOutcome::AlreadyAccounted),
                "concurrent-acknowledged fill must resolve to AlreadyAccounted, got: {outcome:?}"
            );

            // No second fill event and no spurious hedge order from the concurrent path.
            let (fill_count,): (i64,) =
                sqlx::query_as("SELECT COUNT(*) FROM events WHERE event_type = ?")
                    .bind("PositionEvent::OnChainOrderFilled")
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(
                fill_count, 1,
                "Concurrent acknowledged + second process-tx must not emit a second fill event"
            );

            let (order_count,): (i64,) = sqlx::query_as(
                "SELECT COUNT(*) FROM events WHERE event_type LIKE 'OffchainOrderEvent%'",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(
                order_count, 0,
                "Fail-closed on concurrent-acknowledged fill must place no broker order"
            );
        }

        /// Regression test for the crash-window double-count bug:
        ///
        /// 1. Fill A is witnessed and acknowledged in Position (slot = A), but the
        ///    process crashes BEFORE `mark_acknowledged` runs -- OnChainTrade A stays
        ///    Witnessed.
        /// 2. Fill B arrives and is fully processed (slot advances to B).
        /// 3. `process-tx` is retried for A.
        ///
        /// Without the durable `position_fill_already_recorded` guard, the resume
        /// path would call `execute_acknowledge_fill(A)` again. Because the slot now
        /// holds B (not A), `PositionError::DuplicateTrade` does NOT fire and A is
        /// counted a second time -- corrupting the net position.
        ///
        /// After the fix, the retry must:
        /// - Apply fill A exactly once (total fill events = 2: one A + one B).
        /// - Mark OnChainTrade A acknowledged.
        #[tokio::test]
        async fn process_tx_does_not_double_count_witnessed_fill_after_newer_fill_acknowledged() {
            let pool = setup_test_db().await;
            let ctx = create_base_test_ctx();
            let order_placer: Arc<dyn OrderPlacer> = Arc::new(ExecutorOrderPlacer {
                executor: MockExecutor::new(),
                close_flatten_policy: None,
            });

            // Fill A and fill B: same tx_hash, different log_index so they have
            // distinct (tx_hash, log_index) identities.
            let fill_a = onchain_trade_builder().with_block_number(10).build();
            let fill_b = onchain_trade_builder()
                .with_log_index(2)
                .with_block_number(11)
                .build();

            let block_timestamp_a = fill_a.block_timestamp.unwrap();
            let block_timestamp_b = fill_b.block_timestamp.unwrap();

            let trade_id_a = OnChainTradeId::new(Chain::Base, fill_a.tx_hash, fill_a.log_index);
            let trade_id_b = OnChainTradeId::new(Chain::Base, fill_b.tx_hash, fill_b.log_index);

            let (onchain_store, _) = StoreBuilder::<OnChainTradeCqrs>::new(pool.clone())
                .build(())
                .await
                .unwrap();
            let (position_store, _) = StoreBuilder::<Position>::new(pool.clone())
                .build(())
                .await
                .unwrap();

            // Step 1: Witness fill A in OnChainTrade.
            onchain_store
                .send(
                    &trade_id_a,
                    OnChainTradeCommand::Witness {
                        source: fill_a.source,
                        symbol: fill_a.symbol().clone(),
                        amount: fill_a.amount.inner(),
                        direction: fill_a.direction,
                        price_usdc: fill_a.price(),
                        block_number: 10,
                        block_timestamp: block_timestamp_a,
                    },
                )
                .await
                .unwrap();

            // Step 2: Acknowledge fill A in Position (slot = A).
            execute_acknowledge_fill(
                &position_store,
                &fill_a,
                ctx.execution_threshold,
                block_timestamp_a,
            )
            .await
            .unwrap();

            // Simulate crash: do NOT call execute_mark_acknowledged for fill A.
            // OnChainTrade A stays Witnessed; Position already has A applied.

            // Step 3: Fully process fill B (witness + acknowledge + mark).
            onchain_store
                .send(
                    &trade_id_b,
                    OnChainTradeCommand::Witness {
                        source: fill_b.source,
                        symbol: fill_b.symbol().clone(),
                        amount: fill_b.amount.inner(),
                        direction: fill_b.direction,
                        price_usdc: fill_b.price(),
                        block_number: 11,
                        block_timestamp: block_timestamp_b,
                    },
                )
                .await
                .unwrap();

            execute_acknowledge_fill(
                &position_store,
                &fill_b,
                ctx.execution_threshold,
                block_timestamp_b,
            )
            .await
            .unwrap();

            execute_mark_acknowledged(&onchain_store, &trade_id_b)
                .await
                .unwrap();

            // At this point:
            // - Position has fill_a and fill_b applied (last_slot = fill_b's trade_id).
            // - OnChainTrade fill_a is Witnessed (not Acknowledged).
            // - OnChainTrade fill_b is Acknowledged.
            // Without the durable guard, retrying process-tx for fill_a would
            // re-apply it: last_slot (B) != A, so DuplicateTrade does NOT fire.

            // Step 4: Retry process-tx for fill A (crash-recovery scenario).
            process_found_trade(
                fill_a,
                &ctx,
                &pool,
                &stores_for(&pool, &order_placer).await,
                order_placer,
                None,
                None,
            )
            .await
            .unwrap();

            // Assertion 1: OnChainTrade A must now be acknowledged.
            let state_a = onchain_store
                .load(&trade_id_a)
                .await
                .unwrap()
                .expect("OnChainTrade fill_a must exist after process_tx retry");
            assert!(
                state_a.is_acknowledged(),
                "fill_a must be acknowledged after process_tx retry"
            );

            // Assertion 2: Exactly two OnChainOrderFilled events -- fill_a once and
            // fill_b once. Any value other than 2 means double-counting or a dropped
            // fill.
            let (fill_count,): (i64,) = sqlx::query_as(
                "SELECT COUNT(*) FROM events WHERE event_type = 'PositionEvent::OnChainOrderFilled'",
            )
            .fetch_one(&pool)
            .await
            .unwrap();

            assert_eq!(
                fill_count, 2,
                "process_tx retry on a crash-window fill must not double-count: \
                 expected exactly 2 fill events (fill_a once + fill_b once), got {fill_count}"
            );
        }

        /// Regression test for the None-path (fresh-witness) double-count hole:
        ///
        /// A legacy fill whose Position record was written (e.g. via a prior direct
        /// `execute_acknowledge_fill` call) but whose OnChainTrade record was NEVER
        /// created causes `process_found_trade` to take the `None` branch. Without
        /// the unified durable guard the fresh-witness arm would call
        /// `execute_acknowledge_fill` again; because the Position slot already
        /// advanced to a newer fill (B), `DuplicateTrade` does NOT fire and fill A
        /// is counted a second time.
        ///
        /// After the fix the unified `position_fill_already_recorded` guard runs on
        /// every path -- including the `None` path -- and blocks the re-apply.
        #[tokio::test]
        async fn process_tx_none_path_does_not_recount_legacy_position_fill() {
            let pool = setup_test_db().await;
            let ctx = create_base_test_ctx();
            let order_placer: Arc<dyn OrderPlacer> = Arc::new(ExecutorOrderPlacer {
                executor: MockExecutor::new(),
                close_flatten_policy: None,
            });

            // Fill A and fill B have distinct (tx_hash, log_index) identities.
            let fill_a = onchain_trade_builder().with_block_number(10).build();
            let fill_b = onchain_trade_builder()
                .with_log_index(2)
                .with_block_number(11)
                .build();

            let block_timestamp_a = fill_a.block_timestamp.unwrap();
            let block_timestamp_b = fill_b.block_timestamp.unwrap();

            let trade_id_a = OnChainTradeId::new(Chain::Base, fill_a.tx_hash, fill_a.log_index);
            let trade_id_b = OnChainTradeId::new(Chain::Base, fill_b.tx_hash, fill_b.log_index);

            let (onchain_store, _) = StoreBuilder::<OnChainTradeCqrs>::new(pool.clone())
                .build(())
                .await
                .unwrap();
            let (position_store, _) = StoreBuilder::<Position>::new(pool.clone())
                .build(())
                .await
                .unwrap();

            // Step 1: Apply fill A to Position ONLY -- no OnChainTrade witness record.
            // This simulates a legacy fill whose OnChainTrade record was never created.
            // process_found_trade will load None for trade_id_a and take the None path.
            execute_acknowledge_fill(
                &position_store,
                &fill_a,
                ctx.execution_threshold,
                block_timestamp_a,
            )
            .await
            .unwrap();

            // Step 2: Fully process fill B so the Position slot advances beyond A.
            // Now last_acknowledged_trade_id = B, so a re-apply of A bypasses the
            // single-slot DuplicateTrade guard without the durable check.
            onchain_store
                .send(
                    &trade_id_b,
                    OnChainTradeCommand::Witness {
                        source: fill_b.source,
                        symbol: fill_b.symbol().clone(),
                        amount: fill_b.amount.inner(),
                        direction: fill_b.direction,
                        price_usdc: fill_b.price(),
                        block_number: 11,
                        block_timestamp: block_timestamp_b,
                    },
                )
                .await
                .unwrap();

            execute_acknowledge_fill(
                &position_store,
                &fill_b,
                ctx.execution_threshold,
                block_timestamp_b,
            )
            .await
            .unwrap();

            execute_mark_acknowledged(&onchain_store, &trade_id_b)
                .await
                .unwrap();

            // Step 3: Run process_found_trade for fill A. It takes the None branch
            // (no OnChainTrade record), witnesses A, then the authoritative guard must
            // detect A already in Position and skip the re-apply.
            process_found_trade(
                fill_a,
                &ctx,
                &pool,
                &stores_for(&pool, &order_placer).await,
                order_placer,
                None,
                None,
            )
            .await
            .unwrap();

            // Assertion 1: OnChainTrade A must be acknowledged (witness + mark ran).
            let state_a = onchain_store
                .load(&trade_id_a)
                .await
                .unwrap()
                .expect("OnChainTrade fill_a must exist after process_found_trade");
            assert!(
                state_a.is_acknowledged(),
                "fill_a must be acknowledged after process_found_trade takes the None path"
            );

            // Assertion 2: Exactly two fill events -- fill_a once + fill_b once. Any
            // value other than 2 means fill_a was double-counted on the None path.
            let (fill_count,): (i64,) = sqlx::query_as(
                "SELECT COUNT(*) FROM events \
                 WHERE event_type = 'PositionEvent::OnChainOrderFilled'",
            )
            .fetch_one(&pool)
            .await
            .unwrap();

            assert_eq!(
                fill_count, 2,
                "None-path process_tx must not re-apply a fill already in Position: \
                 expected 2 fill events (fill_a once + fill_b once), got {fill_count}"
            );
        }

        /// The happy path: a new fill with trading enabled and a broker that accepts
        /// the order must resolve to a submitted hedge and leave the position with
        /// `pending_offchain_order_id` set (order submitted, not cleared).
        ///
        /// This is the only test that exercises the
        /// `Some(OffchainOrder::Submitted | PartiallyFilled)` arm in
        /// `process_found_trade`.
        #[tokio::test]
        async fn process_tx_submitted_hedge_sets_pending_order_id() {
            let pool = setup_test_db().await;

            // Enable trading so check_execution_readiness can trigger hedge placement.
            let mut ctx = create_base_test_ctx();
            ctx.chains.primary_mut().assets.equities.symbols.insert(
                Symbol::new("AAPL").unwrap(),
                ChainEquityAsset {
                    tokenized_equity: Address::ZERO,
                    tokenized_equity_derivative: Address::ZERO,
                    vault_ids: vec![],
                    trading: OperationMode::Enabled,
                    rebalancing: OperationMode::Disabled,
                    wrapped_equity_recovery: OperationMode::Disabled,
                    operational_limit: None,
                    target_share: None,
                },
            );

            let order_placer: Arc<dyn OrderPlacer> = Arc::new(SucceedingOrderPlacer);

            // 1 share buy -> net +1 -> is_ready_for_execution returns (Sell, 1).
            let onchain_trade = onchain_trade_builder().with_block_number(42).build();

            let outcome = process_found_trade(
                onchain_trade,
                &ctx,
                &pool,
                &stores_for(&pool, &order_placer).await,
                order_placer,
                None,
                None,
            )
            .await
            .unwrap();
            assert!(
                matches!(
                    outcome,
                    ProcessTxOutcome::HedgePlaced {
                        disposition: PlacedHedgeDisposition::InFlight,
                        ..
                    }
                ),
                "successful broker submission must resolve to an in-flight hedge, got: {outcome:?}"
            );

            // pending_offchain_order_id must be set: the order is submitted to the
            // broker and in flight; only the order-status sweep will clear it.
            let (position_store, _) = StoreBuilder::<Position>::new(pool.clone())
                .build(())
                .await
                .unwrap();

            let position = position_store
                .load(&Symbol::new("AAPL").unwrap())
                .await
                .unwrap()
                .expect("Position must exist after fill accounting");

            let pending_order_id = position
                .pending_offchain_order_id
                .expect("pending_offchain_order_id must be set after successful broker submission");

            let (offchain_order_store, _) = StoreBuilder::<OffchainOrder>::new(pool.clone())
                .build(noop_order_placer())
                .await
                .unwrap();
            let offchain_order = offchain_order_store
                .load(&pending_order_id)
                .await
                .unwrap()
                .expect("pending_offchain_order_id must refer to a persisted offchain order");
            assert!(
                matches!(offchain_order, OffchainOrder::Submitted { .. }),
                "pending_offchain_order_id must point to the submitted broker order, got: {offchain_order:?}"
            );

            let (offchain_event_count,): (i64,) = sqlx::query_as(
                "SELECT COUNT(*) FROM events \
                 WHERE aggregate_id = ? AND event_type LIKE 'OffchainOrderEvent%'",
            )
            .bind(pending_order_id.to_string())
            .fetch_one(&pool)
            .await
            .unwrap();
            assert!(
                offchain_event_count >= 1,
                "submitted hedge should persist at least one OffchainOrder event for {pending_order_id}"
            );

            let (fill_count,): (i64,) =
                sqlx::query_as("SELECT COUNT(*) FROM events WHERE event_type = ?")
                    .bind(crate::position::PositionEvent::ON_CHAIN_ORDER_FILLED_EVENT_TYPE)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(
                fill_count, 1,
                "successful broker submission must account the onchain fill exactly once"
            );
        }

        /// A live in-bot submission (poll enrollment `Some`) must enqueue exactly
        /// one `PollOrderStatus` job for the submitted hedge, mirroring the live
        /// placement path so the order is reconciled without waiting for a
        /// startup recovery sweep.
        #[tokio::test]
        async fn process_tx_live_submission_enqueues_one_poll_job() {
            let (pool, apalis_pool) = try_setup_test_pools().await.unwrap();

            let mut ctx = create_base_test_ctx();
            ctx.chains.primary_mut().assets.equities.symbols.insert(
                Symbol::new("AAPL").unwrap(),
                ChainEquityAsset {
                    tokenized_equity: Address::ZERO,
                    tokenized_equity_derivative: Address::ZERO,
                    vault_ids: vec![],
                    trading: OperationMode::Enabled,
                    rebalancing: OperationMode::Disabled,
                    wrapped_equity_recovery: OperationMode::Disabled,
                    operational_limit: None,
                    target_share: None,
                },
            );

            let order_placer: Arc<dyn OrderPlacer> = Arc::new(SucceedingOrderPlacer);
            let poll_status_queue = PollOrderStatusJobQueue::new(&apalis_pool);
            let onchain_trade = onchain_trade_builder().with_block_number(42).build();

            let outcome = process_found_trade(
                onchain_trade,
                &ctx,
                &pool,
                &stores_for(&pool, &order_placer).await,
                order_placer,
                None,
                Some((&poll_status_queue, TEST_POLL_INTERVAL)),
            )
            .await
            .unwrap();

            let ProcessTxOutcome::HedgePlaced {
                offchain_order_id,
                disposition: PlacedHedgeDisposition::InFlight,
                ..
            } = outcome
            else {
                panic!("a live submission must resolve to an in-flight hedge, got: {outcome:?}");
            };

            let poll_job_count: i64 = sqlx_apalis::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM Jobs \
                 WHERE job_type = ? \
                   AND json_extract(CAST(job AS TEXT), '$.offchain_order_id') = ? \
                   AND status IN ('Pending', 'Queued', 'Running')",
            )
            .bind(std::any::type_name::<PollOrderStatus>())
            .bind(offchain_order_id.to_string())
            .fetch_one(&apalis_pool)
            .await
            .unwrap();
            assert_eq!(
                poll_job_count, 1,
                "a live in-bot submission must enqueue exactly one PollOrderStatus job"
            );
        }

        /// `OrderPlacer` whose admission always defers, standing in for the
        /// in-bot placer's schedule aware close flatten policy returning
        /// `Deferred` outside regular session. The broker call is never reached.
        struct DeferringOrderPlacer;

        #[async_trait]
        impl OrderPlacer for DeferringOrderPlacer {
            async fn prepare_placement(
                &self,
                _order: &MarketOrder,
                _kind: &CounterTradeOrderKind,
            ) -> Result<PlacementAdmission, Box<dyn std::error::Error + Send + Sync>> {
                Ok(PlacementAdmission::Deferred)
            }

            async fn place_market_order(
                &self,
                _order: MarketOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                panic!("deferred admission must not reach the broker")
            }

            async fn place_limit_order(
                &self,
                _order: LimitOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                panic!("deferred admission must not reach the broker")
            }

            async fn cancel_order(
                &self,
                _executor_order_id: &ExecutorOrderId,
            ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>> {
                panic!("deferred admission must not cancel")
            }

            async fn preflight_counter_trade_with_reserved_buying_power(
                &self,
                order: MarketOrder,
                _reserved: BuyingPowerReservationCents,
            ) -> Result<CounterTradePreflight, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(reserving_counter_trade_preflight(&order))
            }
        }

        /// Broker admission deferring a fresh placement must not collapse into a
        /// 500 after the position claim and `PlaceReserved` have persisted. Under
        /// Option 2 (ADR 0022) it must settle the fill, fail the still Pending
        /// order, and clear the position claim so the standing pipeline re hedges
        /// from scratch rather than replaying a stale retained intent.
        #[tokio::test]
        async fn process_tx_deferred_broker_admission_clears_pending_claim() {
            let pool = setup_test_db().await;

            let mut ctx = create_base_test_ctx();
            ctx.chains.primary_mut().assets.equities.symbols.insert(
                Symbol::new("AAPL").unwrap(),
                ChainEquityAsset {
                    tokenized_equity: Address::ZERO,
                    tokenized_equity_derivative: Address::ZERO,
                    vault_ids: vec![],
                    trading: OperationMode::Enabled,
                    rebalancing: OperationMode::Disabled,
                    wrapped_equity_recovery: OperationMode::Disabled,
                    operational_limit: None,
                    target_share: None,
                },
            );

            let order_placer: Arc<dyn OrderPlacer> = Arc::new(DeferringOrderPlacer);
            let onchain_trade = onchain_trade_builder().with_block_number(42).build();
            let trade_id =
                OnChainTradeId::new(Chain::Base, onchain_trade.tx_hash, onchain_trade.log_index);
            let position_trade_id = TradeId {
                chain: onchain_trade.chain,
                tx_hash: onchain_trade.tx_hash,
                log_index: onchain_trade.log_index,
            };

            let outcome = process_found_trade(
                onchain_trade,
                &ctx,
                &pool,
                &stores_for(&pool, &order_placer).await,
                order_placer,
                None,
                None,
            )
            .await
            .unwrap();
            let ProcessTxOutcome::HedgePlacementDeferred { symbol } = &outcome else {
                panic!(
                    "deferred broker admission must resolve to a deferred hedge, got: {outcome:?}"
                );
            };
            assert_eq!(symbol, &Symbol::new("AAPL").unwrap());

            // The fill is settled: witnessed, acknowledged, and dropped from the
            // position's pending acknowledgement set.
            let (onchain_trade_store, _) = StoreBuilder::<OnChainTradeCqrs>::new(pool.clone())
                .build(())
                .await
                .unwrap();
            let onchain_state = onchain_trade_store
                .load(&trade_id)
                .await
                .unwrap()
                .expect("the fill must be witnessed after a deferred admission");
            assert!(
                onchain_state.is_acknowledged(),
                "the deferred fill must be acknowledged"
            );

            let (position_store, _) = StoreBuilder::<Position>::new(pool.clone())
                .build(())
                .await
                .unwrap();
            let position = position_store
                .load(&Symbol::new("AAPL").unwrap())
                .await
                .unwrap()
                .expect("the position must exist after fill accounting");
            assert!(
                !position
                    .pending_acknowledged_trade_ids
                    .contains(&position_trade_id),
                "the deferred fill must be settled out of the pending acknowledgement set"
            );

            // The claim is cleared: the position no longer points at a pending
            // offchain order, so the standing pipeline owns the next hedge.
            assert_eq!(
                position.pending_offchain_order_id, None,
                "a deferred admission must clear the pending offchain order claim"
            );

            // A deferral never reaches the broker, so the retired order id is
            // released instead of resting as the idempotency anchor: keeping it
            // would make CheckPositions skip the position and schedule an anchor
            // reconciliation lookup for an order the broker never created.
            assert_eq!(
                position.last_failed_offchain_order_id, None,
                "a never sent deferral must release the failed order anchor"
            );

            // The abandoned order is still driven out of Pending to Failed, so no
            // stuck Pending order is left for a recovery path to replay.
            let (deferred_order_id,): (String,) = sqlx::query_as(
                "SELECT DISTINCT aggregate_id FROM events \
                 WHERE event_type LIKE 'OffchainOrderEvent%'",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            let deferred_order_id: OffchainOrderId = deferred_order_id.parse().unwrap();
            let (offchain_order_store, _) = StoreBuilder::<OffchainOrder>::new(pool.clone())
                .build(noop_order_placer())
                .await
                .unwrap();
            let offchain_order = offchain_order_store
                .load(&deferred_order_id)
                .await
                .unwrap()
                .expect("the deferred offchain order must be persisted");
            assert!(
                matches!(offchain_order, OffchainOrder::Failed { .. }),
                "the deferred order must be failed out of Pending, got: {offchain_order:?}"
            );

            // The terminal is recorded as a deferral, not a hedge failure, so
            // the reliability projection leaves routine close flatten behavior
            // out of the failure counts.
            assert_eq!(
                persisted_failure_kind(&pool).await,
                OffchainOrderFailureKind::Deferral,
                "a schedule deferral must persist as a deferral terminal"
            );
        }

        /// `OrderPlacer` whose admission itself errors (never `Deferred`),
        /// standing in for a broker admission service that is unreachable. The
        /// order is left Pending without ever being sent, so process-tx must
        /// clear the claim exactly as it does for a `Deferred` admission.
        struct AdmissionRejectingOrderPlacer;

        #[async_trait]
        impl OrderPlacer for AdmissionRejectingOrderPlacer {
            async fn prepare_placement(
                &self,
                _order: &MarketOrder,
                _kind: &CounterTradeOrderKind,
            ) -> Result<PlacementAdmission, Box<dyn std::error::Error + Send + Sync>> {
                Err("admission service unavailable".into())
            }

            async fn place_market_order(
                &self,
                _order: MarketOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                panic!("a rejected admission must not reach the broker")
            }

            async fn place_limit_order(
                &self,
                _order: LimitOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                panic!("a rejected admission must not place a limit order")
            }

            async fn cancel_order(
                &self,
                _executor_order_id: &ExecutorOrderId,
            ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>> {
                panic!("a rejected admission must not cancel")
            }

            async fn preflight_counter_trade_with_reserved_buying_power(
                &self,
                order: MarketOrder,
                _reserved: BuyingPowerReservationCents,
            ) -> Result<CounterTradePreflight, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(reserving_counter_trade_preflight(&order))
            }
        }

        /// A broker admission that errors (not a schedule deferral) leaves the
        /// order Pending without ever sending it, so process-tx must clear the
        /// claim and settle the fill just like a `Deferred` admission, then
        /// surface the operational failure rather than stranding a never-sent
        /// Pending order on the position.
        #[tokio::test]
        async fn process_tx_admission_rejection_clears_pending_claim_and_settles() {
            let pool = setup_test_db().await;

            let mut ctx = create_base_test_ctx();
            ctx.chains.primary_mut().assets.equities.symbols.insert(
                Symbol::new("AAPL").unwrap(),
                ChainEquityAsset {
                    tokenized_equity: Address::ZERO,
                    tokenized_equity_derivative: Address::ZERO,
                    vault_ids: vec![],
                    trading: OperationMode::Enabled,
                    rebalancing: OperationMode::Disabled,
                    wrapped_equity_recovery: OperationMode::Disabled,
                    operational_limit: None,
                    target_share: None,
                },
            );

            let order_placer: Arc<dyn OrderPlacer> = Arc::new(AdmissionRejectingOrderPlacer);
            let onchain_trade = onchain_trade_builder().with_block_number(42).build();
            let trade_id =
                OnChainTradeId::new(Chain::Base, onchain_trade.tx_hash, onchain_trade.log_index);
            let position_trade_id = TradeId {
                chain: onchain_trade.chain,
                tx_hash: onchain_trade.tx_hash,
                log_index: onchain_trade.log_index,
            };

            let failed = process_found_trade(
                onchain_trade,
                &ctx,
                &pool,
                &stores_for(&pool, &order_placer).await,
                order_placer,
                None,
                None,
            )
            .await
            .unwrap_err();
            assert!(
                matches!(failed, OperatorError::Operational(_)),
                "an admission rejection must surface as an operational failure, got: {failed:?}"
            );

            let (onchain_trade_store, _) = StoreBuilder::<OnChainTradeCqrs>::new(pool.clone())
                .build(())
                .await
                .unwrap();
            let onchain_state = onchain_trade_store
                .load(&trade_id)
                .await
                .unwrap()
                .expect("the fill must be witnessed after an admission rejection");
            assert!(
                onchain_state.is_acknowledged(),
                "clearing on an admission rejection must settle the fill"
            );

            let (position_store, _) = StoreBuilder::<Position>::new(pool.clone())
                .build(())
                .await
                .unwrap();
            let position = position_store
                .load(&Symbol::new("AAPL").unwrap())
                .await
                .unwrap()
                .expect("the position must exist after fill accounting");
            assert!(
                !position
                    .pending_acknowledged_trade_ids
                    .contains(&position_trade_id),
                "the settled fill must leave the pending acknowledgement set"
            );
            assert_eq!(
                position.pending_offchain_order_id, None,
                "an admission rejection must clear the never sent pending offchain order claim"
            );
            assert_eq!(
                position.last_failed_offchain_order_id, None,
                "a never sent admission rejection must release the failed order anchor"
            );

            // The real admission cause reaches the durable order, not a canned
            // deferral string.
            let (rejected_order_id,): (String,) = sqlx::query_as(
                "SELECT DISTINCT aggregate_id FROM events \
                 WHERE event_type LIKE 'OffchainOrderEvent%'",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            let rejected_order_id: OffchainOrderId = rejected_order_id.parse().unwrap();
            let (offchain_order_store, _) = StoreBuilder::<OffchainOrder>::new(pool.clone())
                .build(noop_order_placer())
                .await
                .unwrap();
            let offchain_order = offchain_order_store
                .load(&rejected_order_id)
                .await
                .unwrap()
                .expect("the rejected offchain order must be persisted");
            let OffchainOrder::Failed { error, .. } = offchain_order else {
                panic!(
                    "an admission rejection must fail the order out of Pending, got: {offchain_order:?}"
                );
            };
            assert!(
                error.contains("admission service unavailable"),
                "the durable failure must record the real admission cause, got: {error}"
            );
            assert!(
                !error.contains("pending intent is retained"),
                "process-tx clears the claim, so the reason must not claim retention, got: {error}"
            );
            assert!(
                error.contains("claim cleared"),
                "the durable failure must state what process-tx did, got: {error}"
            );
            assert_eq!(
                persisted_failure_kind(&pool).await,
                OffchainOrderFailureKind::Failure,
                "an admission failure must persist as a genuine failure terminal"
            );
        }

        /// `OrderPlacer` whose admission passes and whose broker call is rate
        /// limited, standing in for a live broker returning a 429. Unlike a
        /// deferral or an admission failure, the broker WAS contacted here, so
        /// the attempt may have created an order.
        struct RateLimitedOrderPlacer;

        #[async_trait]
        impl OrderPlacer for RateLimitedOrderPlacer {
            async fn place_market_order(
                &self,
                _order: MarketOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                Err(Box::new(AlpacaBrokerApiError::ApiError {
                    status: reqwest::StatusCode::TOO_MANY_REQUESTS,
                    alpaca_code: None,
                    message: "rate limited".to_string(),
                    retry_after: Some(Duration::from_secs(7)),
                }))
            }

            async fn place_limit_order(
                &self,
                _order: LimitOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                panic!("a rate limited market hedge must not place a limit order")
            }

            async fn cancel_order(
                &self,
                _executor_order_id: &ExecutorOrderId,
            ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>> {
                panic!("a rate limited placement must not cancel")
            }

            async fn preflight_counter_trade_with_reserved_buying_power(
                &self,
                order: MarketOrder,
                _reserved: BuyingPowerReservationCents,
            ) -> Result<CounterTradePreflight, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(reserving_counter_trade_preflight(&order))
            }
        }

        /// Broker backpressure differs from a deferral or an admission failure:
        /// the broker WAS contacted, so an order may exist. process-tx still
        /// fails the Pending order, clears the claim, and settles the fill, but
        /// it PRESERVES the idempotency anchor so the next attempt reconciles
        /// whatever the broker may have created. The surfaced failure must keep
        /// its 429 in the chain so the caller's retry policy still classifies it
        /// as backpressure.
        #[tokio::test]
        async fn process_tx_backpressure_clears_pending_claim_and_settles() {
            let pool = setup_test_db().await;

            let mut ctx = create_base_test_ctx();
            ctx.chains.primary_mut().assets.equities.symbols.insert(
                Symbol::new("AAPL").unwrap(),
                ChainEquityAsset {
                    tokenized_equity: Address::ZERO,
                    tokenized_equity_derivative: Address::ZERO,
                    vault_ids: vec![],
                    trading: OperationMode::Enabled,
                    rebalancing: OperationMode::Disabled,
                    wrapped_equity_recovery: OperationMode::Disabled,
                    operational_limit: None,
                    target_share: None,
                },
            );

            let order_placer: Arc<dyn OrderPlacer> = Arc::new(RateLimitedOrderPlacer);
            let onchain_trade = onchain_trade_builder().with_block_number(42).build();
            let trade_id =
                OnChainTradeId::new(Chain::Base, onchain_trade.tx_hash, onchain_trade.log_index);
            let position_trade_id = TradeId {
                chain: onchain_trade.chain,
                tx_hash: onchain_trade.tx_hash,
                log_index: onchain_trade.log_index,
            };

            let failed = process_found_trade(
                onchain_trade,
                &ctx,
                &pool,
                &stores_for(&pool, &order_placer).await,
                order_placer,
                None,
                None,
            )
            .await
            .unwrap_err();
            let OperatorError::Operational(operational) = &failed else {
                panic!(
                    "broker backpressure must surface as an operational failure, got: {failed:?}"
                )
            };
            let backpressure = find_backpressure(operational.as_ref())
                .expect("the surfaced failure must still classify as backpressure");
            assert_eq!(backpressure.retry_after, Some(Duration::from_secs(7)));

            let (onchain_trade_store, _) = StoreBuilder::<OnChainTradeCqrs>::new(pool.clone())
                .build(())
                .await
                .unwrap();
            let onchain_state = onchain_trade_store
                .load(&trade_id)
                .await
                .unwrap()
                .expect("the fill must be witnessed after broker backpressure");
            assert!(
                onchain_state.is_acknowledged(),
                "clearing on backpressure must settle the fill"
            );

            let (position_store, _) = StoreBuilder::<Position>::new(pool.clone())
                .build(())
                .await
                .unwrap();
            let position = position_store
                .load(&Symbol::new("AAPL").unwrap())
                .await
                .unwrap()
                .expect("the position must exist after fill accounting");
            assert!(
                !position
                    .pending_acknowledged_trade_ids
                    .contains(&position_trade_id),
                "the settled fill must leave the pending acknowledgement set"
            );
            assert_eq!(
                position.pending_offchain_order_id, None,
                "backpressure must clear the pending offchain order claim"
            );

            let (rate_limited_order_id,): (String,) = sqlx::query_as(
                "SELECT DISTINCT aggregate_id FROM events \
                 WHERE event_type LIKE 'OffchainOrderEvent%'",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            let rate_limited_order_id: OffchainOrderId = rate_limited_order_id.parse().unwrap();
            assert_eq!(
                position.last_failed_offchain_order_id,
                Some(rate_limited_order_id),
                "backpressure reached the broker, so the anchor must be preserved for \
                 reconciliation"
            );

            let (offchain_order_store, _) = StoreBuilder::<OffchainOrder>::new(pool.clone())
                .build(noop_order_placer())
                .await
                .unwrap();
            let offchain_order = offchain_order_store
                .load(&rate_limited_order_id)
                .await
                .unwrap()
                .expect("the rate limited offchain order must be persisted");
            let OffchainOrder::Failed { error, .. } = offchain_order else {
                panic!("backpressure must fail the order out of Pending, got: {offchain_order:?}")
            };
            assert!(
                error.contains("rate limited"),
                "the durable failure must record the real rate limit cause, got: {error}"
            );
            assert!(
                error.contains("429"),
                "the durable failure must record the broker status of the cause, got: {error}"
            );
            assert!(
                !error.contains("pending intent is retained"),
                "process-tx clears the claim, so the reason must not claim retention, got: {error}"
            );
            assert!(
                error.contains("idempotency anchor"),
                "the durable failure must state that the anchor was preserved, got: {error}"
            );
            assert_eq!(
                persisted_failure_kind(&pool).await,
                OffchainOrderFailureKind::Failure,
                "backpressure must persist as a genuine failure terminal"
            );
        }

        /// A poll enqueue failure while enrolling a freshly placed hedge must
        /// leave the fill unsettled so the retry can enroll again, mirroring the
        /// existing-in-flight branch. The broker order is placed, but the closed
        /// queue fails enrollment before the fill settles; the retry with a
        /// working queue enrolls exactly one job and settles against the in-flight
        /// hedge.
        #[tokio::test]
        async fn process_tx_fresh_placement_poll_enqueue_failure_defers_settlement_until_retry() {
            let (pool, apalis_pool) = try_setup_test_pools().await.unwrap();

            let mut ctx = create_base_test_ctx();
            ctx.chains.primary_mut().assets.equities.symbols.insert(
                Symbol::new("AAPL").unwrap(),
                ChainEquityAsset {
                    tokenized_equity: Address::ZERO,
                    tokenized_equity_derivative: Address::ZERO,
                    vault_ids: vec![],
                    trading: OperationMode::Enabled,
                    rebalancing: OperationMode::Disabled,
                    wrapped_equity_recovery: OperationMode::Disabled,
                    operational_limit: None,
                    target_share: None,
                },
            );

            let order_placer: Arc<dyn OrderPlacer> = Arc::new(SucceedingOrderPlacer);
            let stores = stores_for(&pool, &order_placer).await;
            let poll_status_queue = PollOrderStatusJobQueue::new(&apalis_pool);

            let broken_apalis_pool = try_setup_test_pools().await.unwrap().1;
            broken_apalis_pool.close().await;
            let broken_poll_queue = PollOrderStatusJobQueue::new(&broken_apalis_pool);

            let fill = onchain_trade_builder()
                .with_log_index(1)
                .with_block_number(42)
                .build();
            let trade_id = OnChainTradeId::new(fill.chain, fill.tx_hash, fill.log_index);
            let position_trade_id = TradeId {
                chain: fill.chain,
                tx_hash: fill.tx_hash,
                log_index: fill.log_index,
            };

            // Fresh placement: the broker accepts, but enrolling the poll job on
            // the closed queue fails before the fill settles.
            let failed = process_found_trade(
                fill,
                &ctx,
                &pool,
                &stores,
                order_placer.clone(),
                None,
                Some((&broken_poll_queue, TEST_POLL_INTERVAL)),
            )
            .await
            .unwrap_err();
            assert!(
                matches!(failed, OperatorError::Operational(_)),
                "a broken poll enqueue must surface as an operational failure, got: {failed:?}"
            );

            // The placement itself succeeded, so the position claims the submitted
            // order, but the fill is not settled: witnessed, not acknowledged.
            let (onchain_trade_store, _) = StoreBuilder::<OnChainTradeCqrs>::new(pool.clone())
                .build(())
                .await
                .unwrap();
            let onchain_state = onchain_trade_store
                .load(&trade_id)
                .await
                .unwrap()
                .expect("the fill must be witnessed even when enrollment fails");
            assert!(
                !onchain_state.is_acknowledged(),
                "a failed enrollment must leave the fill unacknowledged for the retry"
            );

            let (position_store, _) = StoreBuilder::<Position>::new(pool.clone())
                .build(())
                .await
                .unwrap();
            let position = position_store
                .load(&Symbol::new("AAPL").unwrap())
                .await
                .unwrap()
                .expect("the position must exist after placement");
            assert!(
                position
                    .pending_acknowledged_trade_ids
                    .contains(&position_trade_id),
                "an unsettled fill must remain in the pending acknowledgement set"
            );
            let offchain_order_id = position
                .pending_offchain_order_id
                .expect("the fresh placement must claim a pending offchain order");

            let poll_job_count_before: i64 = sqlx_apalis::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM Jobs \
                 WHERE job_type = ? \
                   AND json_extract(CAST(job AS TEXT), '$.offchain_order_id') = ? \
                   AND status IN ('Pending', 'Queued', 'Running')",
            )
            .bind(std::any::type_name::<PollOrderStatus>())
            .bind(offchain_order_id.to_string())
            .fetch_one(&apalis_pool)
            .await
            .unwrap();
            assert_eq!(
                poll_job_count_before, 0,
                "a failed enrollment must not enqueue a poll job"
            );

            // The retry with a working queue settles the fill against the now
            // in-flight hedge and enrolls exactly one poll job.
            let fill_retry = onchain_trade_builder()
                .with_log_index(1)
                .with_block_number(42)
                .build();
            let retry_outcome = process_found_trade(
                fill_retry,
                &ctx,
                &pool,
                &stores,
                order_placer,
                None,
                Some((&poll_status_queue, TEST_POLL_INTERVAL)),
            )
            .await
            .unwrap();
            assert!(
                matches!(retry_outcome, ProcessTxOutcome::PendingHedgeInFlight),
                "the retry must settle the fill against the in-flight hedge, got: {retry_outcome:?}"
            );

            let settled_position = position_store
                .load(&Symbol::new("AAPL").unwrap())
                .await
                .unwrap()
                .expect("the position must exist after the retry");
            assert!(
                !settled_position
                    .pending_acknowledged_trade_ids
                    .contains(&position_trade_id),
                "the retry must settle the fill out of the pending acknowledgement set"
            );

            let poll_job_count_after: i64 = sqlx_apalis::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM Jobs \
                 WHERE job_type = ? \
                   AND json_extract(CAST(job AS TEXT), '$.offchain_order_id') = ? \
                   AND status IN ('Pending', 'Queued', 'Running')",
            )
            .bind(std::any::type_name::<PollOrderStatus>())
            .bind(offchain_order_id.to_string())
            .fetch_one(&apalis_pool)
            .await
            .unwrap();
            assert_eq!(
                poll_job_count_after, 1,
                "the retry must enroll exactly one PollOrderStatus job"
            );
        }

        /// A pre-placement `Pending` order is a legitimate deferred retry only
        /// when the trading schedule is enabled. With the schedule disabled the
        /// gate must settle the fill, preserve the claim, and surface a typed
        /// rejection (matching the live path's strict `schedule_enabled` check);
        /// with it enabled it must settle the fill, preserve the claim, and
        /// report the retained deferral.
        #[tokio::test]
        async fn process_tx_retained_pending_gates_on_the_schedule() {
            for schedule_enabled in [false, true] {
                let pool = setup_test_db().await;
                let symbol = Symbol::new("AAPL").unwrap();

                let mut ctx = create_base_test_ctx();
                ctx.chains.primary_mut().assets.equities.symbols.insert(
                    symbol.clone(),
                    ChainEquityAsset {
                        tokenized_equity: Address::ZERO,
                        tokenized_equity_derivative: Address::ZERO,
                        vault_ids: vec![],
                        trading: OperationMode::Enabled,
                        rebalancing: OperationMode::Disabled,
                        wrapped_equity_recovery: OperationMode::Disabled,
                        operational_limit: None,
                        target_share: None,
                    },
                );

                let pending_id = OffchainOrderId::new();
                let block_timestamp = Utc::now();
                // The live pipeline's retained deferred state: the position claims
                // a pending order that rests in `Pending` at the broker.
                let position_store =
                    seed_position_with_pending_order(&pool, &symbol, pending_id, block_timestamp)
                        .await;
                let (offchain_order_store, _) = StoreBuilder::<OffchainOrder>::new(pool.clone())
                    .build(noop_order_placer())
                    .await
                    .unwrap();
                offchain_order_store
                    .send(
                        &pending_id,
                        OffchainOrderCommand::PlaceReserved {
                            symbol: symbol.clone(),
                            shares: positive_shares("1"),
                            direction: Direction::Sell,
                            executor: SupportedExecutor::DryRun,
                            client_order_id: ClientOrderId::from_uuid(uuid::Uuid::new_v4()),
                            kind: CounterTradeOrderKind::Market,
                            buying_power_reservation: None,
                            placed_at: None,
                            provenance: PlacementProvenance::LivePipeline,
                        },
                    )
                    .await
                    .unwrap();

                let later_fill = onchain_trade_builder()
                    .with_log_index(9)
                    .with_block_number(43)
                    .build();
                let later_trade_id =
                    OnChainTradeId::new(later_fill.chain, later_fill.tx_hash, later_fill.log_index);
                let later_position_trade_id = TradeId {
                    chain: later_fill.chain,
                    tx_hash: later_fill.tx_hash,
                    log_index: later_fill.log_index,
                };

                // Placement is never reached: the gate resolves the pre-placement
                // Pending order before any broker call.
                let order_placer = noop_order_placer();
                let mut stores = stores_for(&pool, &order_placer).await;
                stores.schedule_enabled = schedule_enabled;

                let result =
                    process_found_trade(later_fill, &ctx, &pool, &stores, order_placer, None, None)
                        .await;

                if schedule_enabled {
                    let outcome = result.expect("a schedule-enabled retained Pending must defer");
                    let ProcessTxOutcome::PendingHedgeDeferred {
                        symbol: deferred_symbol,
                        offchain_order_id,
                    } = &outcome
                    else {
                        panic!("a retained Pending under a schedule must defer, got: {outcome:?}");
                    };
                    assert_eq!(deferred_symbol, &symbol);
                    assert_eq!(offchain_order_id, &pending_id);
                } else {
                    let error =
                        result.expect_err("a schedule-disabled retained Pending must reject");
                    assert!(
                        matches!(
                            &error,
                            OperatorError::Rejected(RejectionReason::RetainedPendingWithoutSchedule {
                                offchain_order_id,
                                symbol: rejected_symbol,
                            }) if *offchain_order_id == pending_id && rejected_symbol == &symbol
                        ),
                        "a schedule-disabled retained Pending must be a typed rejection, got: {error:?}"
                    );
                }

                // Both branches settle the fill and preserve the claim.
                let (onchain_trade_store, _) = StoreBuilder::<OnChainTradeCqrs>::new(pool.clone())
                    .build(())
                    .await
                    .unwrap();
                let onchain_state = onchain_trade_store
                    .load(&later_trade_id)
                    .await
                    .unwrap()
                    .expect("the later fill must be witnessed");
                assert!(
                    onchain_state.is_acknowledged(),
                    "both branches must settle the later fill (schedule_enabled={schedule_enabled})"
                );

                let position = position_store
                    .load(&symbol)
                    .await
                    .unwrap()
                    .expect("the position must exist");
                assert!(
                    !position
                        .pending_acknowledged_trade_ids
                        .contains(&later_position_trade_id),
                    "both branches must settle the fill out of the pending acknowledgement set \
                     (schedule_enabled={schedule_enabled})"
                );
                assert_eq!(
                    position.pending_offchain_order_id,
                    Some(pending_id),
                    "both branches must preserve the pending claim \
                     (schedule_enabled={schedule_enabled})"
                );
            }
        }

        /// The schedule-disabled refusal carries the same obligation as every
        /// other gate exit: the fill has to be durably settled first. When the
        /// settle cannot be written the gate must surface a retryable
        /// operational failure instead of the typed rejection, leaving the fill
        /// in the position pending acknowledgement set for the retry.
        #[tokio::test]
        async fn retained_pending_rejection_is_operational_when_the_fill_cannot_settle() {
            let pool = setup_test_db().await;
            let symbol = Symbol::new("AAPL").unwrap();
            let ctx = create_base_test_ctx();

            let pending_id = OffchainOrderId::new();
            let position_store =
                seed_position_with_pending_order(&pool, &symbol, pending_id, Utc::now()).await;
            let (offchain_order_store, _) = StoreBuilder::<OffchainOrder>::new(pool.clone())
                .build(noop_order_placer())
                .await
                .unwrap();
            offchain_order_store
                .send(
                    &pending_id,
                    OffchainOrderCommand::PlaceReserved {
                        symbol: symbol.clone(),
                        shares: positive_shares("1"),
                        direction: Direction::Sell,
                        executor: SupportedExecutor::DryRun,
                        client_order_id: ClientOrderId::from_uuid(uuid::Uuid::new_v4()),
                        kind: CounterTradeOrderKind::Market,
                        buying_power_reservation: None,
                        placed_at: None,
                        provenance: PlacementProvenance::LivePipeline,
                    },
                )
                .await
                .unwrap();

            let later_fill = onchain_trade_builder()
                .with_log_index(9)
                .with_block_number(43)
                .build();
            let later_trade_id =
                OnChainTradeId::new(later_fill.chain, later_fill.tx_hash, later_fill.log_index);
            let later_position_trade_id = TradeId {
                chain: later_fill.chain,
                tx_hash: later_fill.tx_hash,
                log_index: later_fill.log_index,
            };
            execute_acknowledge_fill(
                &position_store,
                &later_fill,
                ctx.execution_threshold,
                later_fill.block_timestamp.unwrap(),
            )
            .await
            .unwrap();

            // The gate's only database work is the settle, so running its
            // OnChainTrade marker on a pool closed after the store was built
            // fails exactly that step.
            let severed_pool = setup_test_db().await;
            let order_placer = noop_order_placer();
            let mut stores = stores_for(&pool, &order_placer).await;
            let (severed_store, _) = StoreBuilder::<OnChainTradeCqrs>::new(severed_pool.clone())
                .build(())
                .await
                .unwrap();
            severed_pool.close().await;
            stores.onchain_trade = severed_store;
            assert!(
                !stores.schedule_enabled,
                "the base test context must leave the trading schedule disabled"
            );

            let Err(error) = gate_fill_for_placement(
                &ctx,
                &stores,
                ctx.chains.primary(),
                &later_fill,
                &later_trade_id,
                &symbol,
                None,
            )
            .await
            else {
                panic!("a settle failure must not let the gate report a terminal outcome");
            };

            let OperatorError::Operational(_) = &error else {
                panic!(
                    "an unsettled fill must surface as a retryable operational failure, \
                     not a typed rejection, got: {error:?}"
                );
            };

            let position = position_store
                .load(&symbol)
                .await
                .unwrap()
                .expect("the position must exist after fill accounting");
            assert!(
                position
                    .pending_acknowledged_trade_ids
                    .contains(&later_position_trade_id),
                "an unsettled fill must stay in the pending acknowledgement set for the retry"
            );
        }

        /// The `schedule_enabled` flag a CLI run works from is derived from the
        /// deployed configuration, not hardcoded: a config whose trading
        /// schedule is enforcing must let a leftover Pending hedge settle and
        /// report its deferral, while an observing (or absent) schedule keeps
        /// the typed rejection.
        #[tokio::test]
        async fn standalone_stores_derive_the_schedule_flag_from_config() {
            for schedule_mode in [
                None,
                Some(TradingScheduleMode::Observe),
                Some(TradingScheduleMode::Enabled),
            ] {
                let pool = setup_test_db().await;
                let symbol = Symbol::new("AAPL").unwrap();

                let mut ctx = create_base_test_ctx();
                ctx.chains.primary_mut().assets.equities.symbols.insert(
                    symbol.clone(),
                    ChainEquityAsset {
                        tokenized_equity: Address::ZERO,
                        tokenized_equity_derivative: Address::ZERO,
                        vault_ids: vec![],
                        trading: OperationMode::Enabled,
                        rebalancing: OperationMode::Disabled,
                        wrapped_equity_recovery: OperationMode::Disabled,
                        operational_limit: None,
                        target_share: None,
                    },
                );
                ctx.pricing = Some(pricing_ctx_with_schedule(schedule_mode));

                let pending_id = OffchainOrderId::new();
                let position_store =
                    seed_position_with_pending_order(&pool, &symbol, pending_id, Utc::now()).await;
                let (offchain_order_store, _) = StoreBuilder::<OffchainOrder>::new(pool.clone())
                    .build(noop_order_placer())
                    .await
                    .unwrap();
                offchain_order_store
                    .send(
                        &pending_id,
                        OffchainOrderCommand::PlaceReserved {
                            symbol: symbol.clone(),
                            shares: positive_shares("1"),
                            direction: Direction::Sell,
                            executor: SupportedExecutor::DryRun,
                            client_order_id: ClientOrderId::from_uuid(uuid::Uuid::new_v4()),
                            kind: CounterTradeOrderKind::Market,
                            buying_power_reservation: None,
                            placed_at: None,
                            provenance: PlacementProvenance::LivePipeline,
                        },
                    )
                    .await
                    .unwrap();

                let later_fill = onchain_trade_builder()
                    .with_log_index(9)
                    .with_block_number(43)
                    .build();
                let later_trade_id =
                    OnChainTradeId::new(later_fill.chain, later_fill.tx_hash, later_fill.log_index);

                let order_placer = noop_order_placer();
                let stores = ProcessTxStores::standalone(&pool, &ctx, order_placer.clone())
                    .await
                    .expect("standalone stores must build");
                assert_eq!(
                    stores.schedule_enabled,
                    schedule_mode == Some(TradingScheduleMode::Enabled),
                    "the standalone flag must follow the configured mode ({schedule_mode:?})"
                );

                let result =
                    process_found_trade(later_fill, &ctx, &pool, &stores, order_placer, None, None)
                        .await;

                if schedule_mode == Some(TradingScheduleMode::Enabled) {
                    let outcome = result.expect(
                        "an enforcing schedule must defer the retained \
                                                 Pending",
                    );
                    let ProcessTxOutcome::PendingHedgeDeferred {
                        symbol: deferred_symbol,
                        offchain_order_id,
                    } = &outcome
                    else {
                        panic!("a configured schedule must defer, got: {outcome:?}");
                    };
                    assert_eq!(deferred_symbol, &symbol);
                    assert_eq!(offchain_order_id, &pending_id);
                } else {
                    let error = result.expect_err("an unenforced schedule must reject");
                    assert!(
                        matches!(
                            &error,
                            OperatorError::Rejected(RejectionReason::RetainedPendingWithoutSchedule {
                                offchain_order_id,
                                symbol: rejected_symbol,
                            }) if *offchain_order_id == pending_id && rejected_symbol == &symbol
                        ),
                        "an unenforced schedule must reject the retained Pending ({schedule_mode:?}), got: {error:?}"
                    );
                }

                let (onchain_trade_store, _) = StoreBuilder::<OnChainTradeCqrs>::new(pool.clone())
                    .build(())
                    .await
                    .unwrap();
                let onchain_state = onchain_trade_store
                    .load(&later_trade_id)
                    .await
                    .unwrap()
                    .expect("the later fill must be witnessed");
                assert!(
                    onchain_state.is_acknowledged(),
                    "every branch must settle the later fill ({schedule_mode:?})"
                );

                let position = position_store
                    .load(&symbol)
                    .await
                    .unwrap()
                    .expect("the position must exist");
                assert_eq!(
                    position.pending_offchain_order_id,
                    Some(pending_id),
                    "every branch must preserve the pending claim ({schedule_mode:?})"
                );
            }
        }

        /// A pricing context carrying a trading schedule in `mode`, or none at
        /// all, standing in for the deployed config a CLI run reads.
        fn pricing_ctx_with_schedule(mode: Option<TradingScheduleMode>) -> PricingCtx {
            let mut pricing = PricingCtx::new(
                url::Url::parse("wss://pricing.test/ws").unwrap(),
                "pricing-test-key".to_owned(),
            )
            .unwrap();
            pricing.trading_schedule = mode.map(|mode| {
                let mut config: TradingScheduleConfig = toml::from_str(
                    r#"
                    mode = "observe"
                    environment = "staging"
                    poll_interval_secs = 5
                    request_timeout_secs = 3
                    response_freshness_secs = 30
                    calendar_max_age_secs = 7200
                    evidence_clock_skew_secs = 2
                    emergency_buffer_secs = 900
                    [[scopes]]
                    id = "regular"
                    profile_revision = "v1"
                    extended_hours = false
                    assets = ["AAPL"]
                "#,
                )
                .unwrap();
                config.mode = mode;
                config
            });
            pricing
        }

        /// The decoded-chain guard passes when the decoded fill matches the
        /// requested chain and rejects, naming both chains, when it does not.
        #[test]
        fn ensure_decoded_chain_matches_rejects_a_divergent_chain() {
            ensure_decoded_chain_matches(Chain::Base, Chain::Base)
                .expect("a matching chain must pass the guard");

            let error = ensure_decoded_chain_matches(Chain::Ethereum, Chain::Base).unwrap_err();
            let RejectionReason::DecodedChainMismatch { requested, decoded } = &error else {
                panic!("a divergent chain must be a DecodedChainMismatch, got: {error:?}");
            };
            assert_eq!(*requested, Chain::Base);
            assert_eq!(*decoded, Chain::Ethereum);
            let rendered = error.to_string();
            assert!(
                rendered.contains("base") && rendered.contains("ethereum"),
                "the rejection must name both chains, got: {rendered}"
            );
        }

        /// `OrderPlacer` that defers the first admission and admits every later
        /// one, with a broker that accepts the placement. Stands in for a
        /// schedule where the first process-tx attempt falls outside the regular
        /// session and a later attempt lands inside it.
        struct DeferThenPlaceOrderPlacer {
            prepared: std::sync::atomic::AtomicUsize,
        }

        #[async_trait]
        impl OrderPlacer for DeferThenPlaceOrderPlacer {
            async fn prepare_placement(
                &self,
                _order: &MarketOrder,
                _kind: &CounterTradeOrderKind,
            ) -> Result<PlacementAdmission, Box<dyn std::error::Error + Send + Sync>> {
                if self
                    .prepared
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                    == 0
                {
                    Ok(PlacementAdmission::Deferred)
                } else {
                    Ok(PlacementAdmission::New)
                }
            }

            async fn place_market_order(
                &self,
                order: MarketOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(OrderPlacementResult {
                    executor_order_id: ExecutorOrderId::new("test-broker-order-id"),
                    placed_shares: order.shares,
                    placed_at: Utc::now(),
                    is_extended_hours: false,
                    limit_price: None,
                })
            }

            async fn place_limit_order(
                &self,
                _order: LimitOrder,
            ) -> Result<OrderPlacementResult, Box<dyn std::error::Error + Send + Sync>>
            {
                panic!("the market placement path must not place a limit order")
            }

            async fn cancel_order(
                &self,
                _executor_order_id: &ExecutorOrderId,
            ) -> Result<CancellationOutcome, Box<dyn std::error::Error + Send + Sync>> {
                panic!("placement must not cancel")
            }

            async fn get_order_by_client_order_id(
                &self,
                _client_order_id: &ClientOrderId,
            ) -> Result<Option<BrokerOrderPlacement>, Box<dyn std::error::Error + Send + Sync>>
            {
                // The deferred first attempt never reached the broker, so its
                // preserved anchor has no broker order: the later placement
                // releases the anchor and sizes a fresh hedge.
                Ok(None)
            }

            async fn preflight_counter_trade_with_reserved_buying_power(
                &self,
                order: MarketOrder,
                _reserved: BuyingPowerReservationCents,
            ) -> Result<CounterTradePreflight, Box<dyn std::error::Error + Send + Sync>>
            {
                Ok(reserving_counter_trade_preflight(&order))
            }
        }

        /// After a process-tx admission deferral clears the claim (ADR 0022), a
        /// later fill for the same symbol must run a fresh placement rather than
        /// short circuiting on a retained Pending order. The first attempt defers
        /// and clears; the second is admitted and places a fresh hedge.
        #[tokio::test]
        async fn process_tx_second_fill_after_defer_places_fresh_hedge() {
            let pool = setup_test_db().await;

            let mut ctx = create_base_test_ctx();
            ctx.chains.primary_mut().assets.equities.symbols.insert(
                Symbol::new("AAPL").unwrap(),
                ChainEquityAsset {
                    tokenized_equity: Address::ZERO,
                    tokenized_equity_derivative: Address::ZERO,
                    vault_ids: vec![],
                    trading: OperationMode::Enabled,
                    rebalancing: OperationMode::Disabled,
                    wrapped_equity_recovery: OperationMode::Disabled,
                    operational_limit: None,
                    target_share: None,
                },
            );

            let order_placer: Arc<dyn OrderPlacer> = Arc::new(DeferThenPlaceOrderPlacer {
                prepared: std::sync::atomic::AtomicUsize::new(0),
            });
            let stores = stores_for(&pool, &order_placer).await;

            let first_fill = onchain_trade_builder()
                .with_log_index(1)
                .with_block_number(42)
                .build();
            let first_outcome = process_found_trade(
                first_fill,
                &ctx,
                &pool,
                &stores,
                order_placer.clone(),
                None,
                None,
            )
            .await
            .unwrap();
            assert!(
                matches!(
                    first_outcome,
                    ProcessTxOutcome::HedgePlacementDeferred { .. }
                ),
                "the first fill must defer placement, got: {first_outcome:?}"
            );

            let (position_store, _) = StoreBuilder::<Position>::new(pool.clone())
                .build(())
                .await
                .unwrap();
            let after_first = position_store
                .load(&Symbol::new("AAPL").unwrap())
                .await
                .unwrap()
                .expect("the position must exist after the first fill");
            assert_eq!(
                after_first.pending_offchain_order_id, None,
                "the first defer must clear the pending claim"
            );
            assert_eq!(
                after_first.last_failed_offchain_order_id, None,
                "the first defer never reached the broker, so it must release the anchor"
            );
            let (deferred_order_id,): (String,) = sqlx::query_as(
                "SELECT DISTINCT aggregate_id FROM events \
                 WHERE event_type LIKE 'OffchainOrderEvent%'",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            let deferred_order_id: OffchainOrderId = deferred_order_id.parse().unwrap();

            let second_fill = onchain_trade_builder()
                .with_log_index(2)
                .with_block_number(43)
                .build();
            let second_trade_id = OnChainTradeId::new(
                second_fill.chain,
                second_fill.tx_hash,
                second_fill.log_index,
            );
            let second_position_trade_id = TradeId {
                chain: second_fill.chain,
                tx_hash: second_fill.tx_hash,
                log_index: second_fill.log_index,
            };
            let second_outcome =
                process_found_trade(second_fill, &ctx, &pool, &stores, order_placer, None, None)
                    .await
                    .unwrap();
            let ProcessTxOutcome::HedgePlaced {
                offchain_order_id,
                disposition: PlacedHedgeDisposition::InFlight,
                ..
            } = second_outcome
            else {
                panic!(
                    "the later fill must place a fresh hedge, not short circuit on a retained \
                     Pending, got: {second_outcome:?}"
                );
            };
            assert_ne!(
                offchain_order_id, deferred_order_id,
                "the later fill must place a fresh order, not replay the deferred one"
            );

            // The later fill is settled.
            let (onchain_trade_store, _) = StoreBuilder::<OnChainTradeCqrs>::new(pool.clone())
                .build(())
                .await
                .unwrap();
            let onchain_state = onchain_trade_store
                .load(&second_trade_id)
                .await
                .unwrap()
                .expect("the later fill must be witnessed");
            assert!(
                onchain_state.is_acknowledged(),
                "the later fill must be acknowledged"
            );

            let position = position_store
                .load(&Symbol::new("AAPL").unwrap())
                .await
                .unwrap()
                .expect("the position must exist after the second fill");
            assert!(
                !position
                    .pending_acknowledged_trade_ids
                    .contains(&second_position_trade_id),
                "the later fill must be settled out of the pending acknowledgement set"
            );

            // The fresh hedge is the live claim, in flight at the broker.
            assert_eq!(
                position.pending_offchain_order_id,
                Some(offchain_order_id),
                "the fresh hedge must become the live pending claim"
            );
            let (offchain_order_store, _) = StoreBuilder::<OffchainOrder>::new(pool.clone())
                .build(noop_order_placer())
                .await
                .unwrap();
            let fresh_order = offchain_order_store
                .load(&offchain_order_id)
                .await
                .unwrap()
                .expect("the fresh hedge order must be persisted");
            assert!(
                matches!(fresh_order, OffchainOrder::Submitted { .. }),
                "the fresh hedge must be submitted to the broker, got: {fresh_order:?}"
            );

            // The deferred order stays terminal and is never replayed.
            let deferred_order = offchain_order_store
                .load(&deferred_order_id)
                .await
                .unwrap()
                .expect("the deferred order must be persisted");
            assert!(
                matches!(deferred_order, OffchainOrder::Failed { .. }),
                "the deferred order must remain failed, got: {deferred_order:?}"
            );
        }

        /// An existing in-flight hedge (`Submitted`) discovered before placement
        /// must be enrolled for status polling exactly once, mirroring the live
        /// path, rather than left un-polled until the next startup recovery
        /// sweep. The first fill submits the hedge with no poll enrollment; the
        /// second fill finds it in flight and enrolls it.
        #[tokio::test]
        async fn process_tx_enrolls_poll_job_for_existing_in_flight_hedge() {
            let (pool, apalis_pool) = try_setup_test_pools().await.unwrap();

            let mut ctx = create_base_test_ctx();
            ctx.chains.primary_mut().assets.equities.symbols.insert(
                Symbol::new("AAPL").unwrap(),
                ChainEquityAsset {
                    tokenized_equity: Address::ZERO,
                    tokenized_equity_derivative: Address::ZERO,
                    vault_ids: vec![],
                    trading: OperationMode::Enabled,
                    rebalancing: OperationMode::Disabled,
                    wrapped_equity_recovery: OperationMode::Disabled,
                    operational_limit: None,
                    target_share: None,
                },
            );

            let order_placer: Arc<dyn OrderPlacer> = Arc::new(SucceedingOrderPlacer);
            let stores = stores_for(&pool, &order_placer).await;
            let poll_status_queue = PollOrderStatusJobQueue::new(&apalis_pool);

            let first_fill = onchain_trade_builder()
                .with_log_index(1)
                .with_block_number(42)
                .build();
            let first_outcome = process_found_trade(
                first_fill,
                &ctx,
                &pool,
                &stores,
                order_placer.clone(),
                None,
                None,
            )
            .await
            .unwrap();
            let ProcessTxOutcome::HedgePlaced {
                offchain_order_id,
                disposition: PlacedHedgeDisposition::InFlight,
                ..
            } = first_outcome
            else {
                panic!("the first fill must submit an in-flight hedge, got: {first_outcome:?}");
            };

            let second_fill = onchain_trade_builder()
                .with_log_index(2)
                .with_block_number(43)
                .build();
            let second_outcome = process_found_trade(
                second_fill,
                &ctx,
                &pool,
                &stores,
                order_placer,
                None,
                Some((&poll_status_queue, TEST_POLL_INTERVAL)),
            )
            .await
            .unwrap();
            assert!(
                matches!(second_outcome, ProcessTxOutcome::PendingHedgeInFlight),
                "an existing in-flight hedge must settle the fill without a new placement, got: {second_outcome:?}"
            );

            let poll_job_count: i64 = sqlx_apalis::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM Jobs \
                 WHERE job_type = ? \
                   AND json_extract(CAST(job AS TEXT), '$.offchain_order_id') = ? \
                   AND status IN ('Pending', 'Queued', 'Running')",
            )
            .bind(std::any::type_name::<PollOrderStatus>())
            .bind(offchain_order_id.to_string())
            .fetch_one(&apalis_pool)
            .await
            .unwrap();
            assert_eq!(
                poll_job_count, 1,
                "an existing in-flight hedge must be enrolled for exactly one PollOrderStatus job"
            );
        }

        /// A poll enqueue failure while settling an existing in flight hedge must
        /// leave the fill unsettled so the retry can run enrollment again,
        /// instead of settling first and stranding the poll job when the retry
        /// short circuits as AlreadyAccounted. The first attempt uses a closed
        /// poll queue whose push fails; the retry uses a working queue and must
        /// enroll exactly one job and settle.
        #[tokio::test]
        async fn process_tx_poll_enqueue_failure_defers_settlement_until_retry() {
            let (pool, apalis_pool) = try_setup_test_pools().await.unwrap();

            let mut ctx = create_base_test_ctx();
            ctx.chains.primary_mut().assets.equities.symbols.insert(
                Symbol::new("AAPL").unwrap(),
                ChainEquityAsset {
                    tokenized_equity: Address::ZERO,
                    tokenized_equity_derivative: Address::ZERO,
                    vault_ids: vec![],
                    trading: OperationMode::Enabled,
                    rebalancing: OperationMode::Disabled,
                    wrapped_equity_recovery: OperationMode::Disabled,
                    operational_limit: None,
                    target_share: None,
                },
            );

            let order_placer: Arc<dyn OrderPlacer> = Arc::new(SucceedingOrderPlacer);
            let stores = stores_for(&pool, &order_placer).await;
            let poll_status_queue = PollOrderStatusJobQueue::new(&apalis_pool);

            // A closed apalis pool whose every query and push fails, standing in
            // for a broken queue write during enrollment.
            let broken_apalis_pool = try_setup_test_pools().await.unwrap().1;
            broken_apalis_pool.close().await;
            let broken_poll_queue = PollOrderStatusJobQueue::new(&broken_apalis_pool);

            // The first fill submits an in flight hedge with no enrollment.
            let first_fill = onchain_trade_builder()
                .with_log_index(1)
                .with_block_number(42)
                .build();
            let first_outcome = process_found_trade(
                first_fill,
                &ctx,
                &pool,
                &stores,
                order_placer.clone(),
                None,
                None,
            )
            .await
            .unwrap();
            let ProcessTxOutcome::HedgePlaced {
                offchain_order_id,
                disposition: PlacedHedgeDisposition::InFlight,
                ..
            } = first_outcome
            else {
                panic!("the first fill must submit an in flight hedge, got: {first_outcome:?}");
            };

            // The second fill finds the in flight hedge; the closed queue fails
            // enrollment, so the attempt errors before the fill is settled.
            let second_fill = onchain_trade_builder()
                .with_log_index(2)
                .with_block_number(43)
                .build();
            let second_trade_id = OnChainTradeId::new(
                second_fill.chain,
                second_fill.tx_hash,
                second_fill.log_index,
            );
            let second_position_trade_id = TradeId {
                chain: second_fill.chain,
                tx_hash: second_fill.tx_hash,
                log_index: second_fill.log_index,
            };
            let failed = process_found_trade(
                second_fill,
                &ctx,
                &pool,
                &stores,
                order_placer.clone(),
                None,
                Some((&broken_poll_queue, TEST_POLL_INTERVAL)),
            )
            .await
            .unwrap_err();
            assert!(
                matches!(failed, OperatorError::Operational(_)),
                "a broken poll enqueue must surface as an operational failure, got: {failed:?}"
            );

            // The fill is not settled: the trade is witnessed but not
            // acknowledged, and it is still in the pending acknowledgement set.
            let (onchain_trade_store, _) = StoreBuilder::<OnChainTradeCqrs>::new(pool.clone())
                .build(())
                .await
                .unwrap();
            let onchain_state = onchain_trade_store
                .load(&second_trade_id)
                .await
                .unwrap()
                .expect("the second fill must be witnessed even when enrollment fails");
            assert!(
                !onchain_state.is_acknowledged(),
                "a failed enrollment must leave the fill unacknowledged for the retry"
            );

            let (position_store, _) = StoreBuilder::<Position>::new(pool.clone())
                .build(())
                .await
                .unwrap();
            let position = position_store
                .load(&Symbol::new("AAPL").unwrap())
                .await
                .unwrap()
                .expect("the position must exist after fill accounting");
            assert!(
                position
                    .pending_acknowledged_trade_ids
                    .contains(&second_position_trade_id),
                "an unsettled fill must remain in the pending acknowledgement set"
            );

            let poll_job_count_before: i64 = sqlx_apalis::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM Jobs \
                 WHERE job_type = ? \
                   AND json_extract(CAST(job AS TEXT), '$.offchain_order_id') = ? \
                   AND status IN ('Pending', 'Queued', 'Running')",
            )
            .bind(std::any::type_name::<PollOrderStatus>())
            .bind(offchain_order_id.to_string())
            .fetch_one(&apalis_pool)
            .await
            .unwrap();
            assert_eq!(
                poll_job_count_before, 0,
                "a failed enrollment must not enqueue a poll job"
            );

            // The retry uses a working queue: enrollment succeeds, the fill
            // settles, and exactly one poll job is enrolled.
            let second_fill_retry = onchain_trade_builder()
                .with_log_index(2)
                .with_block_number(43)
                .build();
            let retry_outcome = process_found_trade(
                second_fill_retry,
                &ctx,
                &pool,
                &stores,
                order_placer,
                None,
                Some((&poll_status_queue, TEST_POLL_INTERVAL)),
            )
            .await
            .unwrap();
            assert!(
                matches!(retry_outcome, ProcessTxOutcome::PendingHedgeInFlight),
                "the retry must settle the fill against the in flight hedge, got: {retry_outcome:?}"
            );

            let settled_position = position_store
                .load(&Symbol::new("AAPL").unwrap())
                .await
                .unwrap()
                .expect("the position must exist after the retry");
            assert!(
                !settled_position
                    .pending_acknowledged_trade_ids
                    .contains(&second_position_trade_id),
                "the retry must settle the fill out of the pending acknowledgement set"
            );

            let poll_job_count_after: i64 = sqlx_apalis::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM Jobs \
                 WHERE job_type = ? \
                   AND json_extract(CAST(job AS TEXT), '$.offchain_order_id') = ? \
                   AND status IN ('Pending', 'Queued', 'Running')",
            )
            .bind(std::any::type_name::<PollOrderStatus>())
            .bind(offchain_order_id.to_string())
            .fetch_one(&apalis_pool)
            .await
            .unwrap();
            assert_eq!(
                poll_job_count_after, 1,
                "the retry must enroll exactly one PollOrderStatus job"
            );
        }

        /// A concurrent process-tx and a live trading tick both racing to hedge
        /// the same symbol must place a single broker order. Both converge on the
        /// same Position `PlaceOffChainOrder` gate under the shared
        /// `counter_trade_submission` lock, so the loser is rejected before it
        /// reaches the broker. The second concurrent placement stands in for the
        /// live trading loop, which drives the identical gate and lock.
        ///
        /// The lock must also cover the pre-placement inspection of an existing
        /// pending hedge: without it, the loser can observe the winner's Position
        /// claim before the winner's `OffchainOrder` aggregate exists, misread
        /// the absence as an orphaned pointer, clear the claim, and place a
        /// second hedge. Reproducible under CPU contention (six parallel module
        /// runs) at roughly 5% per run before the lock was widened.
        #[tokio::test]
        async fn concurrent_process_tx_and_tick_place_one_hedge() {
            let pool = setup_test_db().await;

            let mut ctx = create_base_test_ctx();
            ctx.chains.primary_mut().assets.equities.symbols.insert(
                Symbol::new("AAPL").unwrap(),
                ChainEquityAsset {
                    tokenized_equity: Address::ZERO,
                    tokenized_equity_derivative: Address::ZERO,
                    vault_ids: vec![],
                    trading: OperationMode::Enabled,
                    rebalancing: OperationMode::Disabled,
                    wrapped_equity_recovery: OperationMode::Disabled,
                    operational_limit: None,
                    target_share: None,
                },
            );

            let order_placer: Arc<dyn OrderPlacer> = Arc::new(SucceedingOrderPlacer);

            // Two distinct fills for the same symbol so both are accounted and
            // both reach the hedge-placement path, rather than one deduping the
            // other on fill identity.
            let fill_a = onchain_trade_builder().with_block_number(10).build();
            let fill_b = onchain_trade_builder()
                .with_log_index(2)
                .with_block_number(11)
                .build();

            // The one submission lock the conductor shares with every placement
            // path; passing it to both calls is what serializes them.
            let lock = Mutex::new(());
            let stores = stores_for(&pool, &order_placer).await;

            let (outcome_a, outcome_b) = tokio::join!(
                process_found_trade(
                    fill_a,
                    &ctx,
                    &pool,
                    &stores,
                    order_placer.clone(),
                    Some(&lock),
                    None
                ),
                process_found_trade(
                    fill_b,
                    &ctx,
                    &pool,
                    &stores,
                    order_placer.clone(),
                    Some(&lock),
                    None
                ),
            );

            // Exactly one path placed a hedge. The loser either observed the
            // pending hedge (PendingHedgeInFlight / PlacementRejected) or lost the
            // optimistic-concurrency race on an aggregate. Infrastructure and
            // accounting failures are not valid losers.
            let is_aggregate_conflict = |error: &anyhow::Error| {
                error.chain().any(|source| {
                    matches!(
                        source.downcast_ref::<SendError<OnChainTradeCqrs>>(),
                        Some(AggregateError::AggregateConflict)
                    ) || matches!(
                        source.downcast_ref::<SendError<Position>>(),
                        Some(AggregateError::AggregateConflict)
                    ) || matches!(
                        source.downcast_ref::<SendError<OffchainOrder>>(),
                        Some(AggregateError::AggregateConflict)
                    )
                })
            };
            let expected_result = |result: &Result<ProcessTxOutcome, OperatorError>| match result {
                Ok(
                    ProcessTxOutcome::HedgePlaced { .. }
                    | ProcessTxOutcome::PendingHedgeInFlight
                    | ProcessTxOutcome::PlacementRejected { .. },
                ) => true,
                Err(OperatorError::Operational(error)) => is_aggregate_conflict(error),
                Ok(_)
                | Err(
                    OperatorError::Rejected(_) | OperatorError::PreflightReservationMismatch(_),
                ) => false,
            };
            assert!(
                expected_result(&outcome_a) && expected_result(&outcome_b),
                "concurrent loser must be an expected domain outcome or aggregate conflict, \
                 got a={outcome_a:?}, b={outcome_b:?}"
            );

            let placed = |result: &Result<ProcessTxOutcome, OperatorError>| {
                matches!(result, Ok(ProcessTxOutcome::HedgePlaced { .. }))
            };
            let placed_count = usize::from(placed(&outcome_a)) + usize::from(placed(&outcome_b));
            assert_eq!(
                placed_count, 1,
                "exactly one concurrent path may place a hedge, got a={outcome_a:?}, b={outcome_b:?}"
            );

            // And the store holds exactly one offchain order: no double hedge.
            let (order_count,): (i64,) = sqlx::query_as(
                "SELECT COUNT(DISTINCT aggregate_id) FROM events \
                 WHERE event_type LIKE 'OffchainOrderEvent%'",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(
                order_count, 1,
                "concurrent process-tx and tick must place exactly one hedge order, got {order_count}"
            );
        }

        /// Builds a `RebalancingService` over a fresh inventory and a `Position`
        /// store that carries it as a reactor, the way the conductor wires the
        /// bot's own store. Returns the inventory handle so a test can read the
        /// service's live view.
        async fn service_wired_position_store(
            pool: &sqlx::SqlitePool,
        ) -> (
            Arc<crate::inventory::BroadcastingInventory>,
            Arc<st0x_event_sorcery::Store<Position>>,
            Arc<st0x_event_sorcery::Projection<Position>>,
        ) {
            use crate::inventory::{BroadcastingInventory, InventoryView, PollFreshness};
            use crate::rebalancing::{
                ChainRebalancingConfig, RebalancingSchedulers, RebalancingService,
                RebalancingServiceConfig,
            };
            use crate::vault_registry::{VaultRegistry, VaultRegistryId};

            let (_pool, apalis_pool) = try_setup_test_pools().await.expect("test pools must build");
            let (event_sender, _) = tokio::sync::broadcast::channel(16);
            // A funded view: the fill's USDC leg debits the market-making cash
            // balance, and the equity leg is a delta on the existing holding,
            // as in production.
            let inventory = Arc::new(BroadcastingInventory::new(
                InventoryView::default()
                    .with_equity(
                        Symbol::new("AAPL").unwrap(),
                        FractionalShares::new(st0x_float_macro::float!(10)),
                        FractionalShares::new(st0x_float_macro::float!(10)),
                    )
                    .with_usdc(
                        st0x_finance::Usdc::new(st0x_float_macro::float!(10_000)),
                        st0x_finance::Usdc::new(st0x_float_macro::float!(10_000)),
                    ),
                event_sender,
            ));
            let (vault_registry, _) = StoreBuilder::<VaultRegistry>::new(pool.clone())
                .build(())
                .await
                .unwrap();
            let service = Arc::new(RebalancingService::new(
                RebalancingServiceConfig {
                    poll_freshness: PollFreshness::always_fresh(),
                    inventory_staleness_bound: std::time::Duration::from_secs(300),
                    cash_reserved: None,
                    hedge_floor: st0x_execution::HedgeFloor::default(),
                    allocation: st0x_config::AllocationCtx::base_test(),
                    usdc: None,
                    transfer_timeout: std::time::Duration::from_secs(60),
                    chains: std::collections::BTreeMap::from([(
                        Chain::Base,
                        ChainRebalancingConfig::for_test(ChainAssets {
                            equities: crate::test_utils::rebalancing_enabled_equities(&["AAPL"]),
                            cash: None,
                        }),
                    )]),
                },
                vault_registry,
                std::collections::BTreeMap::from([(
                    Chain::Base,
                    VaultRegistryId {
                        chain: Chain::Base,
                        orderbook: Address::ZERO,
                        owner: Address::ZERO,
                    },
                )]),
                inventory.clone(),
                std::collections::BTreeMap::from([(
                    Chain::Base,
                    Arc::new(st0x_wrapper::MockWrapper::new()) as Arc<dyn st0x_wrapper::Wrapper>,
                )]),
                RebalancingSchedulers::new(&apalis_pool),
                Arc::new(crate::alerts::LogNotifier),
            ));

            let (position, position_projection) = StoreBuilder::<Position>::new(pool.clone())
                .with(service)
                .build(())
                .await
                .unwrap();
            (inventory, position, position_projection)
        }

        /// The in-bot route must write through the conductor's wired `Position`
        /// store: with the `RebalancingService` reactor attached, processing a
        /// fill applies it to the service's inventory and arms the symbol's
        /// pending-order gate at once, so a rebalancing check that runs before
        /// the next inventory poll sees the live balances and the open hedge.
        /// A detached store (the pre-fix route) leaves both untouched until
        /// polling repairs them, a window in which a check acts on stale state.
        #[tokio::test]
        async fn wired_position_store_updates_rebalancing_inventory_immediately() {
            let pool = setup_test_db().await;
            let symbol = Symbol::new("AAPL").unwrap();
            let mut ctx = create_base_test_ctx();
            ctx.chains.primary_mut().assets.equities.symbols.insert(
                symbol.clone(),
                ChainEquityAsset {
                    tokenized_equity: Address::ZERO,
                    tokenized_equity_derivative: Address::ZERO,
                    vault_ids: vec![],
                    trading: OperationMode::Enabled,
                    rebalancing: OperationMode::Disabled,
                    wrapped_equity_recovery: OperationMode::Disabled,
                    operational_limit: None,
                    target_share: None,
                },
            );
            let order_placer: Arc<dyn OrderPlacer> = Arc::new(SucceedingOrderPlacer);

            let (inventory, position, position_projection) =
                service_wired_position_store(&pool).await;
            let standalone = stores_for(&pool, &order_placer).await;
            let stores = ProcessTxStores {
                onchain_trade: standalone.onchain_trade,
                position,
                position_projection,
                offchain_order: standalone.offchain_order,
                schedule_enabled: standalone.schedule_enabled,
            };

            // 1 share buy at 150 -> net +1 -> the opposite hedge is placed.
            let onchain_trade = onchain_trade_builder().with_block_number(42).build();
            let outcome = process_found_trade(
                onchain_trade,
                &ctx,
                &pool,
                &stores,
                order_placer,
                None,
                None,
            )
            .await
            .unwrap();
            assert!(
                matches!(outcome, ProcessTxOutcome::HedgePlaced { .. }),
                "got {outcome:?}"
            );

            // No inventory poll has run: the reactor alone must have applied the
            // fill and armed the gate.
            let (market_making, gate_armed) = {
                let view = inventory.read().await;
                (
                    view.equity_available(&symbol, crate::inventory::Venue::MarketMaking),
                    view.has_pending_offchain_order(&symbol),
                )
            };
            assert_eq!(
                market_making,
                Some(FractionalShares::new(st0x_float_macro::float!(11))),
                "the on-chain buy must land in the market-making equity balance before any poll"
            );
            assert!(
                gate_armed,
                "the placed hedge must arm the symbol's pending-order gate before any poll"
            );
        }

        /// The contrast that makes the wired-store requirement observable: the
        /// same fill through standalone stores never reaches the service.
        #[tokio::test]
        async fn detached_position_store_leaves_rebalancing_inventory_stale() {
            let pool = setup_test_db().await;
            let symbol = Symbol::new("AAPL").unwrap();
            let mut ctx = create_base_test_ctx();
            ctx.chains.primary_mut().assets.equities.symbols.insert(
                symbol.clone(),
                ChainEquityAsset {
                    tokenized_equity: Address::ZERO,
                    tokenized_equity_derivative: Address::ZERO,
                    vault_ids: vec![],
                    trading: OperationMode::Enabled,
                    rebalancing: OperationMode::Disabled,
                    wrapped_equity_recovery: OperationMode::Disabled,
                    operational_limit: None,
                    target_share: None,
                },
            );
            let order_placer: Arc<dyn OrderPlacer> = Arc::new(SucceedingOrderPlacer);
            let (inventory, _wired_position, _wired_projection) =
                service_wired_position_store(&pool).await;

            let onchain_trade = onchain_trade_builder().with_block_number(42).build();
            let outcome = process_found_trade(
                onchain_trade,
                &ctx,
                &pool,
                &stores_for(&pool, &order_placer).await,
                order_placer,
                None,
                None,
            )
            .await
            .unwrap();
            assert!(matches!(outcome, ProcessTxOutcome::HedgePlaced { .. }));

            let (market_making, gate_armed) = {
                let view = inventory.read().await;
                (
                    view.equity_available(&symbol, crate::inventory::Venue::MarketMaking),
                    view.has_pending_offchain_order(&symbol),
                )
            };
            assert_eq!(
                market_making,
                Some(FractionalShares::new(st0x_float_macro::float!(10))),
                "a detached store never reaches the service; the balance stays at its seed"
            );
            assert!(!gate_armed);
        }
    }
}

pub mod rebalancing {
    pub use crate::rebalancing::to_wrapped_equities;

    pub mod equity {
        pub use crate::rebalancing::equity::{
            ChainEquityServices, CrossVenueEquityTransfer, EquityTransferServices,
        };
    }

    pub mod usdc {
        pub use crate::rebalancing::usdc::{
            CrossVenueCashTransfer, MarketMakingUsdcEndpoints, UsdcSettlementParams,
            UsdcTransferError,
        };
    }
}

pub mod telemetry {
    pub use crate::telemetry::TelemetrySender;

    pub mod broker {
        pub use crate::telemetry::broker::InstrumentedAlpacaBroker;
    }
}

pub mod tokenized_equity_mint {
    pub use crate::tokenized_equity_mint::{TokenizedEquityMint, TokenizedEquityMintCommand};
}

#[cfg(feature = "test-support")]
pub mod trading {
    pub mod offchain {
        pub mod close_flatten {
            pub use crate::trading::offchain::close_flatten::{
                CloseFlattenCrossRamp, CloseFlattenPolicy,
            };
        }

        pub mod hedge {
            pub use crate::trading::offchain::hedge::HedgeJobQueue;
        }
    }

    pub mod onchain {
        pub mod inclusion {
            pub use crate::trading::onchain::inclusion::EmittedOnChain;
        }

        pub mod trade_accountant {
            pub use crate::trading::onchain::trade_accountant::TradeAccountingError;
        }
    }
}

pub mod usdc_rebalance {
    pub use crate::usdc_rebalance::{
        RebalanceDirection, ReconcileReason, UsdcRebalance, UsdcRebalanceCommand, UsdcRebalanceId,
    };

    #[cfg(feature = "test-support")]
    pub use crate::usdc_rebalance::{ConversionAmounts, TransferRef};
}

pub mod vault_lookup {
    pub use crate::vault_lookup::{VaultLookup, VaultRegistryLookup};

    #[cfg(feature = "test-support")]
    pub use crate::vault_lookup::MockVaultLookup;
}

pub mod vault_registry {
    pub use crate::vault_registry::{VaultRegistry, VaultRegistryId};
}

#[cfg(feature = "test-support")]
pub mod bindings {
    pub use crate::bindings::IRaindexV6;
}

#[cfg(feature = "test-support")]
pub mod test_utils {
    pub use crate::test_utils::{
        OnchainTradeBuilder, TEST_POLL_INTERVAL, get_test_order, mock_alpaca_broker_ctx,
        try_positive_shares, try_rebalancing_enabled_equities, try_setup_test_db,
        try_setup_test_pools,
    };
}

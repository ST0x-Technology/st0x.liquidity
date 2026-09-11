//! Narrow application boundary used by the operator CLI.
//!
//! The CLI is a separate application crate. This module exposes only the
//! domain operations and types that application needs while keeping the
//! implementation modules themselves private.

use chrono::{DateTime, NaiveDate, Utc};
use st0x_execution::{FractionalShares, Symbol};

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
        "existing pending offchain order {offchain_order_id} for {symbol} is still Pending \
         before placement; refusing to clear the position claim"
    )]
    OffchainOrderStillPendingBeforePlacement {
        offchain_order_id: OffchainOrderId,
        symbol: Symbol,
    },
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
}

/// The failure of a shared operator recovery command, letting a caller-facing
/// rejection and an operational failure map to different results.
#[derive(Debug, thiserror::Error)]
pub enum OperatorError {
    /// The request cannot be applied in the aggregate's current state, or an
    /// input was invalid; the caller surfaces this to the operator.
    #[error(transparent)]
    Rejected(#[from] RejectionReason),
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
            OffchainOrder, OffchainOrderCommand, OffchainOrderError, OffchainOrderId,
            OffchainOrderPlacement, OrderPlacementResult, OrderPlacer,
            TerminalPositionFinalization, client_order_id_for_placement,
            place_offchain_order_at_broker, position_command_for_finalization,
            terminal_position_finalization,
        };

        #[cfg(feature = "test-support")]
        pub use crate::offchain::order::{
            CancellationReason, CounterTradeOrderKind, OffchainOrderEvent, PollOrderStatusJobQueue,
            noop_order_placer,
        };
    }
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

    use crate::offchain::order::{OffchainOrder, OffchainOrderId};
    use crate::operator::{OperatorError, RejectionReason};

    pub use crate::position::{AnchorDisposition, Position, PositionCommand};

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

    use alloy::primitives::TxHash;
    use alloy::providers::Provider;
    use anyhow::Context;
    use sqlx::SqlitePool;
    use tokio::sync::Mutex;
    use tracing::{error, info};

    use st0x_config::Ctx;
    use st0x_event_sorcery::{Projection, Store, StoreBuilder};
    use st0x_evm::ReadOnlyEvm;
    use st0x_execution::{Direction, FractionalShares, MockExecutor, Positive, Symbol};
    use st0x_registry::SymbolCache;

    use crate::conductor::{
        FillAccountingOutcome, account_for_onchain_fill, execute_mark_acknowledged,
        execute_settle_fill, is_expected_place_offchain_order_rejection,
    };
    use crate::offchain::order::{
        OffchainOrder, OffchainOrderId, OffchainOrderPlacement, OrderPlacer,
        TerminalPositionFinalization, client_order_id_for_placement,
        place_offchain_order_at_broker, position_command_for_finalization,
        terminal_position_finalization,
    };
    use crate::onchain::accumulator::check_execution_readiness;
    use crate::onchain::trade::{BotOperator, RecoveryActors};
    use crate::onchain::{OnChainError, OnchainTrade, TradeValidationError};
    use crate::onchain_trade::{OnChainTrade, OnChainTradeId};
    use crate::position::{AnchorDisposition, Position, PositionCommand};

    use super::{OperatorError, RejectionReason};

    /// The state of a hedge order after (attempted) broker placement, or of an
    /// existing pending hedge found before placement.
    #[derive(Debug, Clone, Copy)]
    pub enum HedgeDisposition {
        /// The broker accepted the order; the next order-status recovery sweep
        /// reconciles it to a terminal state.
        InFlight,
        /// Placement failed or the order vanished; the position's pending marker
        /// was cleared so the normal pipeline can re-hedge.
        ClearedForRetry,
        /// The order reached a terminal broker state and the position was
        /// finalized.
        Finalized,
    }

    /// What processing a transaction's fill resolved to.
    #[derive(Debug)]
    pub enum ProcessTxOutcome {
        /// No orderbook events in the transaction matched the configured order.
        NoTradeableEvents,
        /// The RPC endpoint did not find the transaction.
        TransactionNotFound { tx_hash: TxHash },
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
        /// A hedge order was placed at the broker.
        HedgePlaced {
            symbol: Symbol,
            offchain_order_id: OffchainOrderId,
            shares: Positive<FractionalShares>,
            direction: Direction,
            disposition: HedgeDisposition,
        },
    }

    /// The three stores a process-tx writes through.
    ///
    /// In the bot process these are the conductor's wired stores, so every
    /// event the fill produces reaches the running reactors: the
    /// `RebalancingService` applies the fill to its inventory and arms the
    /// pending-order gate immediately, rather than after the next inventory
    /// poll. The offline CLI has no reactors to reach and builds standalone
    /// stores with default projections.
    #[derive(Clone)]
    pub struct ProcessTxStores {
        pub onchain_trade: Arc<Store<OnChainTrade>>,
        pub position: Arc<Store<Position>>,
        pub position_projection: Arc<Projection<Position>>,
        pub offchain_order: Arc<Store<OffchainOrder>>,
    }

    impl ProcessTxStores {
        /// Standalone stores with default projections and no reactors, for a
        /// process with no running bot to dispatch to.
        pub async fn standalone(
            pool: &SqlitePool,
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
            })
        }
    }

    /// Accounts a missed on-chain fill from `tx_hash` and, when the resulting
    /// net exposure warrants it, places the opposite hedge.
    ///
    /// Run inside the bot process, pass the conductor's wired `stores` (so the
    /// fill reaches the running reactors) and the live `submission_lock` so the
    /// pending-hedge inspection and the broker placement serialize against the
    /// trading loop (ADR 0014); under the lock, the shared `Position`
    /// aggregate's pending-order gate prevents a racing tick from double-placing
    /// the hedge. The CLI runs in a separate process with standalone stores, no
    /// shared lock, and passes `None`.
    pub async fn process_tx<P: Provider + Clone + 'static>(
        tx_hash: TxHash,
        ctx: &Ctx,
        pool: &SqlitePool,
        provider: &P,
        cache: &SymbolCache,
        stores: &ProcessTxStores,
        order_placer: Arc<dyn OrderPlacer>,
        submission_lock: Option<&Mutex<()>>,
    ) -> Result<ProcessTxOutcome, OperatorError> {
        let trading_chain = ctx.chains.primary();
        let actors = RecoveryActors {
            order_owner: ctx.vault_owner(),
            bot_operator: BotOperator(ctx.order_owner()),
        };
        let read_evm = ReadOnlyEvm::new(provider.clone());

        match OnchainTrade::try_from_tx_hash(tx_hash, &read_evm, cache, trading_chain, actors).await
        {
            Ok(Some(onchain_trade)) => {
                process_found_trade(
                    onchain_trade,
                    ctx,
                    pool,
                    stores,
                    order_placer,
                    submission_lock,
                )
                .await
            }
            Ok(None) => Ok(ProcessTxOutcome::NoTradeableEvents),
            Err(OnChainError::Validation(TradeValidationError::TransactionNotFound(_))) => {
                Ok(ProcessTxOutcome::TransactionNotFound { tx_hash })
            }
            Err(error) => Err(OperatorError::Operational(anyhow::Error::new(error))),
        }
    }

    async fn process_found_trade(
        onchain_trade: OnchainTrade,
        ctx: &Ctx,
        pool: &SqlitePool,
        stores: &ProcessTxStores,
        order_placer: Arc<dyn OrderPlacer>,
        submission_lock: Option<&Mutex<()>>,
    ) -> Result<ProcessTxOutcome, OperatorError> {
        let trade_id = OnChainTradeId::new(
            onchain_trade.chain,
            onchain_trade.tx_hash,
            onchain_trade.log_index,
        );

        let ProcessTxStores {
            onchain_trade: onchain_trade_store,
            position: position_store,
            position_projection,
            offchain_order: offchain_order_store,
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

        match reconcile_existing_pending_order(offchain_order_store, position_store, base_symbol)
            .await?
        {
            None | Some(HedgeDisposition::ClearedForRetry | HedgeDisposition::Finalized) => {}
            Some(HedgeDisposition::InFlight) => {
                mark_and_settle_fill(
                    onchain_trade_store,
                    position_store,
                    &trade_id,
                    &onchain_trade,
                )
                .await?;
                return Ok(ProcessTxOutcome::PendingHedgeInFlight);
            }
        }

        let trading_enabled = ctx.chains.primary().assets.is_trading_enabled(base_symbol);

        if !trading_enabled {
            mark_and_settle_fill(
                onchain_trade_store,
                position_store,
                &trade_id,
                &onchain_trade,
            )
            .await?;
            return Ok(ProcessTxOutcome::TradingDisabled {
                symbol: base_symbol.clone(),
            });
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
            &ctx.chains.primary().assets,
            &ctx.assets,
            trading_enabled,
        )
        .await
        .context("failed to check execution readiness")?
        else {
            mark_and_settle_fill(
                onchain_trade_store,
                position_store,
                &trade_id,
                &onchain_trade,
            )
            .await?;
            return Ok(ProcessTxOutcome::BelowExecutionThreshold);
        };

        let offchain_order_id = OffchainOrderId::new();

        let anchor = position_store
            .load(&params.symbol)
            .await
            .inspect_err(|error| {
                error!(
                    %offchain_order_id,
                    symbol = %params.symbol,
                    %error,
                    "Failed to load position for the idempotency anchor; refusing \
                     placement until it can be read"
                );
            })
            .context("failed to load position for the idempotency anchor")?
            .and_then(|position| position.last_failed_offchain_order_id);

        // `_submission_guard` above is still held here, so the aggregate claim
        // and the broker placement are serialized against the trading loop.

        match position_store
            .send(
                &params.symbol,
                PositionCommand::PlaceOffChainOrder {
                    offchain_order_id,
                    shares: params.shares,
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
                mark_and_settle_fill(
                    onchain_trade_store,
                    position_store,
                    &trade_id,
                    &onchain_trade,
                )
                .await?;
                return Ok(ProcessTxOutcome::PlacementRejected {
                    symbol: params.symbol.clone(),
                });
            }
            Err(error) => return Err(OperatorError::Operational(anyhow::Error::new(error))),
        }

        let client_order_id = client_order_id_for_placement(offchain_order_id, anchor);

        place_offchain_order_at_broker(
            offchain_order_store,
            order_placer.as_ref(),
            &offchain_order_id,
            OffchainOrderPlacement::market(
                params.symbol.clone(),
                params.shares,
                params.direction,
                params.executor,
                client_order_id,
            ),
        )
        .await
        .context("failed to place the offchain order at the broker")?;

        let disposition = reconcile_post_place_state(
            offchain_order_store,
            position_store,
            &params.symbol,
            offchain_order_id,
        )
        .await?;

        mark_and_settle_fill(
            onchain_trade_store,
            position_store,
            &trade_id,
            &onchain_trade,
        )
        .await?;

        Ok(ProcessTxOutcome::HedgePlaced {
            symbol: params.symbol.clone(),
            offchain_order_id,
            shares: params.shares,
            direction: params.direction,
            disposition,
        })
    }

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

    /// Inspects any pending hedge already recorded on the position before
    /// placing a new one. `None` means no pending order; otherwise the returned
    /// disposition reports whether it is still live (`InFlight`) or was resolved
    /// (`ClearedForRetry`/`Finalized`) so the caller may place a fresh hedge.
    async fn reconcile_existing_pending_order(
        offchain_order_store: &Store<OffchainOrder>,
        position_store: &Store<Position>,
        symbol: &Symbol,
    ) -> Result<Option<HedgeDisposition>, OperatorError> {
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
        .map(Some)
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
                        },
                    )
                    .await
                    .context("failed to clear the missing offchain order from the position")?;
                Ok(HedgeDisposition::ClearedForRetry)
            }
            Some(OffchainOrder::Pending { .. }) => match context {
                PlacementContext::PrePlacement => {
                    Err(RejectionReason::OffchainOrderStillPendingBeforePlacement {
                        offchain_order_id,
                        symbol: symbol.clone(),
                    }
                    .into())
                }
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

        use alloy::primitives::{Address, B256, U256};
        use async_trait::async_trait;
        use chrono::Utc;
        use tokio::sync::Mutex;

        use st0x_config::{
            ChainAssets, ChainEquityAsset, Ctx, ExecutionThreshold, HedgingAssets, OperationMode,
        };
        use st0x_event_sorcery::StoreBuilder;
        use st0x_evm::Chain;
        use st0x_execution::{
            CancellationOutcome, Direction, ExecutorOrderId, FractionalShares, LimitOrder,
            MarketOrder, MockExecutor, Positive, SupportedExecutor, Symbol,
        };

        use crate::bindings::IRaindexV6::{ClearConfigV2, ClearV3};
        use crate::conductor::{
            TradeProcessingCqrs, execute_acknowledge_fill, execute_mark_acknowledged,
            process_queued_trade,
        };
        use crate::offchain::order::{
            CancellationReason, ExecutorOrderPlacer, OffchainOrder, OffchainOrderId,
            OrderPlacementResult, OrderPlacer, PollOrderStatusJobQueue, RetainedFill,
            noop_order_placer,
        };
        use crate::onchain::trade::RaindexTradeEvent;
        use crate::onchain_trade::{
            InventoryVenue, OnChainTrade as OnChainTradeCqrs, OnChainTradeCommand, OnChainTradeId,
            OnChainTradeSource,
        };
        use crate::position::{Position, PositionCommand};
        use crate::test_utils::{
            OnchainTradeBuilder, TEST_POLL_INTERVAL, get_test_order, try_positive_shares,
            try_setup_test_db, try_setup_test_pools,
        };
        use crate::trading::onchain::inclusion::EmittedOnChain;
        use crate::trading::onchain::trade_accountant::TradeAccountingError;

        use super::{
            HedgeDisposition, OperatorError, PlacementContext, ProcessTxOutcome, ProcessTxStores,
            RejectionReason, process_found_trade, reconcile_offchain_order_state,
            reconcile_post_place_state,
        };

        fn positive_shares(value: &str) -> Positive<FractionalShares> {
            try_positive_shares(value).expect("test shares must be valid and positive")
        }

        async fn setup_test_db() -> sqlx::SqlitePool {
            try_setup_test_db()
                .await
                .expect("test database setup must succeed")
        }

        fn onchain_trade_builder() -> OnchainTradeBuilder {
            OnchainTradeBuilder::try_new().expect("default onchain trade fixture must be valid")
        }

        fn create_base_test_ctx() -> Ctx {
            st0x_config::create_test_ctx_with_order_owner(Address::ZERO)
        }

        /// Standalone stores for the offline-path tests; the reactor-wired
        /// path is covered by
        /// `wired_position_store_updates_rebalancing_inventory_immediately`.
        async fn stores_for(
            pool: &sqlx::SqlitePool,
            order_placer: &Arc<dyn OrderPlacer>,
        ) -> ProcessTxStores {
            ProcessTxStores::standalone(pool, order_placer.clone())
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
                },
            );

            let order_placer: Arc<dyn OrderPlacer> =
                Arc::new(ExecutorOrderPlacer(MockExecutor::new()));

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
            let order_placer: Arc<dyn OrderPlacer> =
                Arc::new(ExecutorOrderPlacer(MockExecutor::new()));

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
            let order_placer: Arc<dyn OrderPlacer> =
                Arc::new(ExecutorOrderPlacer(MockExecutor::new()));

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
            let order_placer: Arc<dyn OrderPlacer> =
                Arc::new(ExecutorOrderPlacer(MockExecutor::new()));

            let onchain_trade = onchain_trade_builder().with_block_number(42).build();

            // Step 1: process-tx applies the fill.
            process_found_trade(
                onchain_trade.clone(),
                &ctx,
                &pool,
                &stores_for(&pool, &order_placer).await,
                order_placer.clone(),
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
            let order_placer: Arc<dyn OrderPlacer> =
                Arc::new(ExecutorOrderPlacer(MockExecutor::new()));

            let onchain_trade = onchain_trade_builder().with_block_number(None).build();

            let error = process_found_trade(
                onchain_trade,
                &ctx,
                &pool,
                &stores_for(&pool, &order_placer).await,
                order_placer,
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
            let order_placer: Arc<dyn OrderPlacer> =
                Arc::new(ExecutorOrderPlacer(MockExecutor::new()));

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
            )
            .await
            .unwrap();
            assert!(
                matches!(
                    outcome,
                    ProcessTxOutcome::HedgePlaced {
                        disposition: HedgeDisposition::ClearedForRetry,
                        ..
                    }
                ),
                "failed placement must resolve to a hedge cleared for retry, got: {outcome:?}"
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

        #[tokio::test]
        async fn pending_order_refusal_names_the_placement_phase() {
            for context in [
                PlacementContext::PrePlacement,
                PlacementContext::PostPlacement,
            ] {
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
                    market_session: st0x_execution::MarketSession::Regular,
                    close_flatten: false,
                };

                let error = reconcile_offchain_order_state(
                    Some(pending_order),
                    &position_store,
                    &symbol,
                    offchain_order_id,
                    context,
                )
                .await
                .unwrap_err();
                let reason = match error {
                    OperatorError::Rejected(reason) => reason,
                    other @ OperatorError::Operational(_) => panic!(
                        "a Pending order must surface as a typed rejection for {context:?}, got: {other}"
                    ),
                };
                let names_phase = match (context, &reason) {
                    (
                        PlacementContext::PrePlacement,
                        RejectionReason::OffchainOrderStillPendingBeforePlacement {
                            offchain_order_id: id,
                            symbol: rejected,
                        },
                    )
                    | (
                        PlacementContext::PostPlacement,
                        RejectionReason::OffchainOrderUnexpectedPostPlacementState {
                            offchain_order_id: id,
                            symbol: rejected,
                        },
                    ) => *id == offchain_order_id && *rejected == symbol,
                    _ => false,
                };
                assert!(
                    names_phase,
                    "the rejection must carry the placement phase and the order for {context:?}, got: {reason:?}"
                );

                let position = position_store
                    .load(&symbol)
                    .await
                    .unwrap()
                    .expect("position should exist after setup");
                assert_eq!(
                    position.pending_offchain_order_id,
                    Some(offchain_order_id),
                    "a refusal must leave the position claim in place for {context:?}"
                );
            }
        }

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
            let order_placer: Arc<dyn OrderPlacer> =
                Arc::new(ExecutorOrderPlacer(MockExecutor::new()));

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
                },
            );

            let order_placer: Arc<dyn OrderPlacer> =
                Arc::new(ExecutorOrderPlacer(MockExecutor::new()));

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
            let order_placer: Arc<dyn OrderPlacer> =
                Arc::new(ExecutorOrderPlacer(MockExecutor::new()));

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
            let order_placer: Arc<dyn OrderPlacer> =
                Arc::new(ExecutorOrderPlacer(MockExecutor::new()));

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
            )
            .await
            .unwrap();
            assert!(
                matches!(
                    outcome,
                    ProcessTxOutcome::HedgePlaced {
                        disposition: HedgeDisposition::InFlight,
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
                    Some(&lock)
                ),
                process_found_trade(
                    fill_b,
                    &ctx,
                    &pool,
                    &stores,
                    order_placer.clone(),
                    Some(&lock)
                ),
            );

            // Exactly one path placed a hedge. The loser either observed the
            // pending hedge (PendingHedgeInFlight / PlacementRejected) or lost the
            // optimistic-concurrency race on the shared Position aggregate
            // (aggregate conflict, retried upstream); never a second placement.
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
            use crate::inventory::{
                BroadcastingInventory, ImbalanceThreshold, InventoryView, PollFreshness,
            };
            use crate::rebalancing::{
                RebalancingSchedulers, RebalancingService, RebalancingServiceConfig,
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
                    equity: ImbalanceThreshold {
                        target: st0x_float_macro::float!(0.5),
                        deviation: st0x_float_macro::float!(0.2),
                    },
                    usdc: None,
                    transfer_timeout: std::time::Duration::from_secs(60),
                    assets: ChainAssets {
                        equities: crate::test_utils::rebalancing_enabled_equities(&["AAPL"]),
                        cash: None,
                    },
                },
                vault_registry,
                VaultRegistryId {
                    chain: Chain::Base,
                    orderbook: Address::ZERO,
                    owner: Address::ZERO,
                },
                inventory.clone(),
                Arc::new(st0x_wrapper::MockWrapper::new()),
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
            };

            // 1 share buy at 150 -> net +1 -> the opposite hedge is placed.
            let onchain_trade = onchain_trade_builder().with_block_number(42).build();
            let outcome =
                process_found_trade(onchain_trade, &ctx, &pool, &stores, order_placer, None)
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

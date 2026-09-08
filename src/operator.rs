//! Narrow application boundary used by the operator CLI.
//!
//! The CLI is a separate application crate. This module exposes only the
//! domain operations and types that application needs while keeping the
//! implementation modules themselves private.

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

    use anyhow::{Context, bail, ensure};
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
    ) -> anyhow::Result<SetEquityMarkOutcome> {
        let &EquityMarkCorrection {
            day,
            ref symbol,
            usd_mark,
            observed_at,
            ref source,
            ref reason,
        } = correction;
        ensure!(
            !source.trim().is_empty(),
            "--source must not be blank; it is persisted as audit provenance"
        );
        ensure!(
            !reason.trim().is_empty(),
            "--reason must not be blank; it is persisted as the audit record"
        );

        let capture_boundary = New_York
            .from_local_datetime(
                &day.and_hms_opt(0, 5, 0)
                    .context("invalid ET capture time")?,
            )
            .single()
            .context("ambiguous ET capture boundary")?
            .with_timezone(&Utc);
        if observed_at >= capture_boundary {
            bail!(
                "--observed-at must identify the regular-session close before the {day} 00:05 ET \
                 capture boundary ({capture_boundary})"
            );
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
                bail!(
                    "{symbol} has no [chains.<name>.trading.assets.equities] entry, so its {unconverted} \
                     wrapped-location row(s) on {day} hold vault shares, not underlying shares. \
                     A mark would price them as underlying and misstate the day's capital. \
                     Reconcile the holding instead, or restore the config entry so the capture \
                     can resolve a vault ratio."
                );
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
            bail!(
                "historical mark event committed, but the portfolio-snapshot read model did not \
                 update every {day} {symbol} row; run `view rebuild --aggregate \
                 portfolio-snapshot --all` before retrying"
            );
        }

        Ok(SetEquityMarkOutcome { formatted_mark })
    }
}

pub mod position {
    use anyhow::{Context, bail, ensure};
    use rain_math_float::Float;
    use sqlx::SqlitePool;
    use st0x_config::ExecutionThreshold;
    use st0x_event_sorcery::{StoreBuilder, load_entity};
    use st0x_execution::{FractionalShares, Symbol};

    use crate::offchain::order::{OffchainOrder, OffchainOrderId};

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
    ) -> anyhow::Result<SetPositionOutcome> {
        ensure!(
            !reason.trim().is_empty(),
            "--reason must not be blank; it is persisted as the audit record"
        );

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
            bail!(
                "position {symbol} has pending offchain order {pending}; \
                 run position release-hedge before setting position"
            );
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
    /// Shared by the operator CLI and the ops API. Operates directly on the
    /// database: the caller must ensure the bot is not concurrently driving the
    /// same order, since a fill landing between the state read and the commands
    /// here cannot be guarded against.
    pub async fn release_pending_offchain_order(
        pool: &SqlitePool,
        symbol: &Symbol,
        offchain_order_id: OffchainOrderId,
        reason: &str,
    ) -> anyhow::Result<ReleaseHedgeOutcome> {
        ensure!(
            !reason.trim().is_empty(),
            "--reason must not be blank; it is persisted as the audit record"
        );

        let (position, projection) = StoreBuilder::<Position>::new(pool.clone())
            .build(())
            .await
            .context("failed to build position store")?;

        let Some(view) = projection
            .load(symbol)
            .await
            .context("failed to load position view")?
        else {
            bail!("position {symbol} not found");
        };

        let order = load_entity::<OffchainOrder>(pool, &offchain_order_id)
            .await
            .context("failed to load offchain order aggregate")?;

        if let Some(existing) = &order {
            if existing.symbol() != symbol {
                bail!(
                    "OffchainOrder {offchain_order_id} belongs to {}, not {symbol} -- refusing \
                     to repair",
                    existing.symbol()
                );
            }
            match existing {
                OffchainOrder::PartiallyFilled { .. } => {
                    bail!(
                        "OffchainOrder {offchain_order_id} is PartiallyFilled: shares already \
                         executed offchain, and failing it would erase that hedge from the \
                         position. Reconcile the partial fill first."
                    );
                }
                OffchainOrder::Filled { .. } => {
                    bail!(
                        "OffchainOrder {offchain_order_id} is Filled: the hedge executed. This \
                         command cannot repair a filled order -- reconcile the fill into the \
                         position instead of failing it."
                    );
                }
                OffchainOrder::Cancelling { .. } | OffchainOrder::Cancelled { .. } => {
                    bail!(
                        "OffchainOrder {offchain_order_id} is in a cancellation lifecycle state: \
                         this command fails stuck Pending/Submitted orders, not cancellations -- \
                         refusing. Confirm the intended recovery path for cancellation states."
                    );
                }
                OffchainOrder::Pending { .. }
                | OffchainOrder::Submitted { .. }
                | OffchainOrder::Failed { .. } => {}
            }
        }

        match view.pending_offchain_order_id {
            Some(pending) if pending == offchain_order_id => {}
            Some(pending) => {
                bail!(
                    "position {symbol} pending offchain order is {pending}, not {offchain_order_id}"
                );
            }
            None => {
                if order.is_none() {
                    bail!(
                        "position {symbol} has no pending offchain order and no OffchainOrder \
                         aggregate {offchain_order_id} exists -- nothing to repair"
                    );
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

        position
            .send(
                symbol,
                PositionCommand::FailOffChainOrder {
                    offchain_order_id,
                    error: reason.to_string(),
                    // The repaired order is typically still live at the broker
                    // (this force-fails stuck Pending/Submitted orders, not
                    // confirmed broker-terminal ones); releasing here would
                    // re-arm the double-hedge the anchor exists to prevent.
                    anchor: AnchorDisposition::Preserve,
                },
            )
            .await
            .context("failed to fail pending offchain order")?;

        // Pointer-first: the pending pointer is cleared above. Now drive the
        // OffchainOrder aggregate itself to its Failed terminal so it does not
        // linger as a live-looking order in the view. The two aggregates are not
        // transactionally atomic (separate CQRS boundaries); this second step is
        // idempotent -- an already-terminal or absent order is left as-is.
        let offchain_order =
            detail::fail_offchain_order_aggregate(pool, order, offchain_order_id, reason).await?;

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

        use anyhow::{Context, bail};
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

        /// What the repair must do with a freshly re-loaded `OffchainOrder` state.
        ///
        /// The "executed shares always escalate" rule is the load-bearing
        /// financial-safety invariant of this command; classifying the state in one
        /// place keeps the pre-send guard and the post-send `AlreadyCompleted`
        /// recovery from encoding it differently and silently diverging.
        pub enum ReloadOutcome {
            /// Executed shares present (`Filled`/`PartiallyFilled`): failing the order
            /// would erase a hedge the position no longer accounts for. The caller must
            /// refuse and route the operator to manual reconciliation.
            Escalate,
            /// Already `Failed`: a benign concurrent terminal transition. Report it and
            /// leave the existing failure record untouched.
            BenignTerminal,
            /// No executed shares and not terminal (`Pending`/`Submitted`/absent). The
            /// pre-send guard proceeds to `MarkFailed`; the post-`AlreadyCompleted` site
            /// treats it as an unreachable invariant violation.
            Proceed,
        }

        /// Single source of the executed-shares-escalate rule shared by both re-load
        /// sites in [`fail_offchain_order_aggregate`].
        pub fn classify_reloaded_state(state: Option<&OffchainOrder>) -> ReloadOutcome {
            use OffchainOrder::{
                Cancelled, Cancelling, Failed, Filled, PartiallyFilled, Pending, Submitted,
            };

            match state {
                // Executed shares (Filled/PartiallyFilled) would erase a hedge; and a
                // concurrent transition into a cancellation lifecycle state during a
                // fail must route to manual reconciliation rather than being failed
                // blind (a Cancelled order may carry a partial fill). Confirm the
                // intended recovery path for cancellation states.
                Some(
                    Filled { .. } | PartiallyFilled { .. } | Cancelling { .. } | Cancelled { .. },
                ) => ReloadOutcome::Escalate,
                Some(Failed { .. }) => ReloadOutcome::BenignTerminal,
                Some(Pending { .. } | Submitted { .. }) | None => ReloadOutcome::Proceed,
            }
        }

        /// Drives the standalone `OffchainOrder` aggregate (pre-loaded by the caller)
        /// to its `Failed` terminal via `MarkFailed`, after its position pointer has
        /// been cleared.
        ///
        /// Routed through the wired store so `offchain_order_view`
        /// updates immediately. Idempotent: an already-`Failed` or absent order is
        /// reported and left untouched rather than erroring, so a partial prior run
        /// can be re-run safely; `Filled`/`PartiallyFilled` orders are refused because
        /// failing them would erase executed hedge shares.
        pub async fn fail_offchain_order_aggregate(
            pool: &SqlitePool,
            order: Option<OffchainOrder>,
            offchain_order_id: OffchainOrderId,
            reason: &str,
        ) -> anyhow::Result<OffchainOrderOutcome> {
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
                    bail!(
                        "OffchainOrder {offchain_order_id} has executed shares (state {order:?}) -- \
                     refusing to erase the executed hedge"
                    );
                }
                Cancelling { .. } | Cancelled { .. } => {
                    bail!(
                        "OffchainOrder {offchain_order_id} is in a cancellation lifecycle state \
                     (state {order:?}): this command fails Pending/Submitted orders, not \
                     cancellations -- refusing. Confirm the intended recovery path."
                    );
                }
                Pending { .. } | Submitted { .. } => {}
            }

            // Re-load immediately before sending: the caller's snapshot may be stale,
            // and MarkFailed is a legal transition from PartiallyFilled at the
            // aggregate level (the bot's own post-partial-fill rejection path needs
            // it), so a partial fill landing since the snapshot would otherwise be
            // erased SILENTLY. This narrows the race window to the load->send gap.
            //
            // CONTRACT: this command requires that the bot is not concurrently driving
            // this order (see the caller docstring). Honoring that contract means no
            // event can land in the load->send gap, so the re-load is exact. The
            // defenses for a contract violation are best-effort and ASYMMETRIC:
            //   - a complete fill in the gap makes MarkFailed return AlreadyCompleted,
            //     which the post-send handler below escalates;
            //   - a PARTIAL fill in the gap does NOT -- MarkFailed succeeds from
            //     PartiallyFilled -- so it would be erased silently and is UNGUARDED.
            // That sliver is closed only by honoring the no-concurrent-bot contract;
            // there is no in-process guard for it because the aggregate must keep
            // MarkFailed legal from PartiallyFilled for the bot's own rejection path.
            let current = load_entity::<OffchainOrder>(pool, &offchain_order_id)
                .await
                .context("failed to re-load offchain order before MarkFailed")?;
            match classify_reloaded_state(current.as_ref()) {
                ReloadOutcome::Escalate => {
                    bail!(
                        "OffchainOrder {offchain_order_id} acquired executed shares concurrently; \
                     the position pointer may already be cleared -- reconcile the position \
                     manually instead of failing the order"
                    );
                }
                ReloadOutcome::BenignTerminal => {
                    return Ok(OffchainOrderOutcome::TerminalConcurrently);
                }
                ReloadOutcome::Proceed => {}
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
                    OffchainOrderCommand::MarkFailed {
                        error: reason.to_string(),
                        filled_shares: None,
                        failed_at: chrono::Utc::now(),
                    },
                )
                .await;

            match send_result {
                Ok(()) => Ok(OffchainOrderOutcome::MarkedFailed),
                // The bot can transition the order to a terminal state in the sliver
                // between the re-load above and this command; a concurrent FAIL is
                // equivalent to finding it terminal up front, but a concurrent FILL
                // means the pointer was cleared for an order that actually executed --
                // surface that as a hard error so the operator reconciles the
                // position instead of trusting a clean exit.
                Err(AggregateError::UserError(LifecycleError::Apply(
                    OffchainOrderError::AlreadyCompleted,
                ))) => {
                    let terminal = load_entity::<OffchainOrder>(pool, &offchain_order_id)
                        .await
                        .context("failed to re-load offchain order after concurrent transition")?;
                    match classify_reloaded_state(terminal.as_ref()) {
                        // Executed shares always escalate: PartiallyFilled cannot
                        // produce AlreadyCompleted today, but if it ever does, the
                        // same pointer-cleared-without-accounting hazard applies.
                        ReloadOutcome::Escalate => {
                            bail!(
                                "OffchainOrder {offchain_order_id} acquired executed shares \
                             concurrently: the position pointer was cleared without accounting \
                             the fill -- reconcile the position manually"
                            );
                        }
                        ReloadOutcome::BenignTerminal => {
                            Ok(OffchainOrderOutcome::TerminalConcurrently)
                        }
                        // MarkFailed only returns AlreadyCompleted from a terminal
                        // aggregate (Filled or Failed), so a non-terminal or absent
                        // state here means the order regressed out of a terminal state
                        // -- impossible under the append-only lifecycle. Bail loudly as
                        // an invariant violation rather than silently reporting a clean
                        // "left as-is".
                        ReloadOutcome::Proceed => {
                            bail!(
                                "OffchainOrder {offchain_order_id} returned AlreadyCompleted from \
                             MarkFailed but re-loaded as a non-terminal state -- aggregate \
                             lifecycle invariant violated"
                            );
                        }
                    }
                }
                Err(error) => {
                    Err(anyhow::Error::new(error).context("failed to mark offchain order failed"))
                }
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
                error
                    .to_string()
                    .contains("acquired executed shares concurrently"),
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

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
    pub use crate::portfolio_snapshot::{
        PortfolioBalanceRowWithMark, PortfolioSnapshot, PortfolioSnapshotCommand,
        PortfolioSnapshotId, PortfolioSnapshotProjection,
    };
}

pub mod position {
    pub use crate::position::{AnchorDisposition, Position, PositionCommand};

    #[cfg(feature = "test-support")]
    pub use crate::position::{PositionEvent, TradeId};
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

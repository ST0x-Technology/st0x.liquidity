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
    use sqlx::SqlitePool;
    use std::time::Duration;

    use st0x_config::Ctx;
    use st0x_event_sorcery::{AggregateError, load_entity, send_command};
    use st0x_tokenization::IssuerRequestId;

    use crate::equity_redemption::{
        DetectionFailure, EquityRedemption, EquityRedemptionCommand, RedemptionAggregateId,
    };
    use crate::rebalancing::equity::EquityTransferServices;
    use crate::tokenized_equity_mint::{TokenizedEquityMint, TokenizedEquityMintCommand};

    /// The equity-transfer aggregate targeted by an operator recovery.
    #[derive(Debug, Clone, Copy)]
    pub enum EquityTransferKind {
        Mint,
        Redemption,
    }

    fn stale_state_context<Failure: std::error::Error + Send + Sync + 'static>(
        kind: &str,
        id: &str,
        error: AggregateError<Failure>,
    ) -> anyhow::Error {
        match error {
            rejection @ (AggregateError::UserError(_) | AggregateError::AggregateConflict) => {
                anyhow::Error::new(rejection).context(format!(
                    "{kind} {id} rejected the failure command. The state may have \
                     advanced since it was read (is the bot driving this aggregate \
                     concurrently?) -- re-run to see the current state."
                ))
            }
            infrastructure @ (AggregateError::DatabaseConnectionError(_)
            | AggregateError::DeserializationError(_)
            | AggregateError::UnexpectedError(_)) => anyhow::Error::new(infrastructure),
        }
    }

    /// Exposes stale-state error mapping to application-boundary tests.
    #[cfg(feature = "test-support")]
    pub fn stale_state_context_for_test<Failure: std::error::Error + Send + Sync + 'static>(
        kind: &str,
        id: &str,
        error: AggregateError<Failure>,
    ) -> anyhow::Error {
        stale_state_context(kind, id, error)
    }

    /// Marks a stuck mint or redemption aggregate as failed.
    pub async fn fail_transfer(
        pool: &SqlitePool,
        transfer_kind: EquityTransferKind,
        id: &str,
        reason: &str,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            !reason.trim().is_empty(),
            "--reason must not be blank; it is persisted as the audit record"
        );

        let services = EquityTransferServices::panicking();

        match transfer_kind {
            EquityTransferKind::Mint => {
                let mint_id: IssuerRequestId = id
                    .parse()
                    .map_err(|error| anyhow::anyhow!("Invalid mint id {id:?}: {error}"))?;
                let entity = load_entity::<TokenizedEquityMint>(pool, &mint_id)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("Mint aggregate not found: {id}"))?;
                let command = match entity {
                    TokenizedEquityMint::MintRequested { .. }
                    | TokenizedEquityMint::MintAccepted { .. } => {
                        TokenizedEquityMintCommand::FailAcceptance {
                            reason: reason.to_string(),
                        }
                    }
                    TokenizedEquityMint::TokensReceived { .. }
                    | TokenizedEquityMint::WrapSubmitted { .. } => {
                        TokenizedEquityMintCommand::FailWrapping {
                            reason: reason.to_string(),
                        }
                    }
                    TokenizedEquityMint::TokensWrapped { .. }
                    | TokenizedEquityMint::VaultDepositSubmitted { .. } => {
                        TokenizedEquityMintCommand::FailRaindexDeposit {
                            reason: reason.to_string(),
                        }
                    }
                    TokenizedEquityMint::DepositedIntoRaindex { .. } => {
                        anyhow::bail!("Mint {id} already completed (DepositedIntoRaindex)");
                    }
                    TokenizedEquityMint::Failed { .. } => {
                        anyhow::bail!("Mint {id} already failed");
                    }
                    TokenizedEquityMint::Reconciled { .. } => {
                        anyhow::bail!("Mint {id} already reconciled");
                    }
                };

                send_command::<TokenizedEquityMint>(pool, &mint_id, command, services)
                    .await
                    .map_err(|error| stale_state_context("Mint", id, error))?;
            }
            EquityTransferKind::Redemption => {
                let redemption_id: RedemptionAggregateId = id
                    .parse()
                    .map_err(|error| anyhow::anyhow!("Invalid redemption ID: {error}"))?;
                let entity = load_entity::<EquityRedemption>(pool, &redemption_id)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("Redemption aggregate not found: {id}"))?;
                let command = match entity {
                    EquityRedemption::VaultWithdrawPending { .. }
                    | EquityRedemption::VaultWithdrawSubmitted { .. }
                    | EquityRedemption::WithdrawnFromRaindex { .. }
                    | EquityRedemption::UnwrapPending { .. }
                    | EquityRedemption::UnwrapSubmitted { .. }
                    | EquityRedemption::TokensUnwrapped { .. }
                    | EquityRedemption::SendPending { .. } => {
                        EquityRedemptionCommand::FailTransfer {
                            reason: reason.to_string(),
                        }
                    }
                    EquityRedemption::TokensSent { .. } => EquityRedemptionCommand::FailDetection {
                        failure: DetectionFailure::Operator {
                            reason: reason.to_string(),
                        },
                    },
                    EquityRedemption::Pending { .. } => EquityRedemptionCommand::RejectRedemption {
                        reason: reason.to_string(),
                    },
                    EquityRedemption::Completed { .. } => {
                        anyhow::bail!("Redemption {id} already completed");
                    }
                    EquityRedemption::Failed { .. } => {
                        anyhow::bail!("Redemption {id} already failed");
                    }
                    EquityRedemption::Reconciled { .. } => {
                        anyhow::bail!("Redemption {id} already reconciled");
                    }
                };

                send_command::<EquityRedemption>(pool, &redemption_id, command, services)
                    .await
                    .map_err(|error| stale_state_context("Redemption", id, error))?;
            }
        }

        Ok(())
    }

    /// Returns the local server endpoint used to re-check a transfer.
    pub fn recheck_url(ctx: &Ctx, transfer_kind: EquityTransferKind, id: &str) -> String {
        let kind = match transfer_kind {
            EquityTransferKind::Mint => "equity_mint",
            EquityTransferKind::Redemption => "equity_redemption",
        };

        format!(
            "http://127.0.0.1:{}/transfers/recheck/{kind}/{id}",
            ctx.server_port
        )
    }

    /// Requests an in-process re-check and returns its operator-facing outcome.
    pub async fn recheck_transfer(
        ctx: &Ctx,
        transfer_kind: EquityTransferKind,
        id: &str,
    ) -> anyhow::Result<String> {
        let url = recheck_url(ctx, transfer_kind, id);
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;
        let response = client.post(url.as_str()).send().await.map_err(|error| {
            if error.is_connect() {
                anyhow::Error::new(error)
                    .context(format!("could not reach the bot at {url}; is it running?"))
            } else {
                anyhow::Error::new(error)
            }
        })?;
        let status = response.status();
        let body = response.text().await?;

        if !status.is_success() {
            anyhow::bail!("transfer recheck failed ({status}): {body}");
        }

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
        use sqlx::SqlitePool;

        use super::{EquityTransferKind, fail_transfer};

        #[tokio::test]
        async fn fail_transfer_rejects_blank_audit_reasons_before_dispatch() {
            let pool = SqlitePool::connect_lazy("sqlite::memory:").unwrap();

            for reason in ["", " ", "\t\n"] {
                let error = fail_transfer(&pool, EquityTransferKind::Mint, "unused", reason)
                    .await
                    .unwrap_err();

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
    pub use crate::native_gas::GasReadiness;
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
        pub use crate::rebalancing::equity::{CrossVenueEquityTransfer, EquityTransferServices};
    }

    pub mod usdc {
        pub use crate::rebalancing::usdc::{
            CrossVenueCashTransfer, UsdcSettlementParams, UsdcTransferError,
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
    pub use crate::usdc_rebalance::TransferRef;
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
        OnchainTradeBuilder, TEST_POLL_INTERVAL, get_test_order, try_positive_shares,
        try_rebalancing_enabled_equities, try_setup_test_db, try_setup_test_pools,
    };
}

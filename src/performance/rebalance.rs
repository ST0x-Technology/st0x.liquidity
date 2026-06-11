//! Rebalance stage timing read model derived from the CQRS event store.
//!
//! Folds each `UsdcRebalance` aggregate's event stream into a per-operation
//! stage breakdown (conversion, withdrawal, CCTP burn -> attestation -> mint,
//! deposit) so the dashboard can show how long full liquidity roundtrips
//! take and trend Circle's attestation time. Strictly read-only.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use sqlx::SqlitePool;
use tracing::warn;

use st0x_dto::{
    AttestationSample, RebalanceOperationTiming, RebalanceStageName, RebalanceStageStats,
    RebalanceStageTiming, RebalanceTimingStatus, RebalanceTimings,
};

use super::{PerformanceError, ReportRange, latency_stats};
use crate::usdc_rebalance::{RebalanceDirection, UsdcRebalanceEvent};

/// Stage-breakdown rows returned per report; the full operation count is
/// still reported via `total_operations`.
const MAX_OPERATION_REPORTS: usize = 100;

/// Load rebalance stage timings for operations started within `range`.
///
/// Undeserializable events are skipped with a warning rather than failing
/// the whole report.
pub(crate) async fn load_rebalance_timings(
    pool: &SqlitePool,
    range: &ReportRange,
) -> Result<RebalanceTimings, PerformanceError> {
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT aggregate_id, payload FROM events \
         WHERE aggregate_type = 'UsdcRebalance' \
         ORDER BY aggregate_id, sequence",
    )
    .fetch_all(pool)
    .await?;

    let mut streams: BTreeMap<String, Vec<UsdcRebalanceEvent>> = BTreeMap::new();
    for (aggregate_id, payload) in rows {
        match serde_json::from_str(&payload) {
            Ok(event) => streams.entry(aggregate_id).or_default().push(event),
            Err(error) => {
                warn!(%aggregate_id, %error, "Skipping undeserializable UsdcRebalance event");
            }
        }
    }

    let operations = streams
        .into_iter()
        .filter_map(|(operation_id, events)| fold_operation(operation_id, &events))
        .collect();

    Ok(rebalance_timing_report(operations, range))
}

/// Assemble the dashboard report from folded operations.
pub(crate) fn rebalance_timing_report(
    operations: Vec<RebalanceOperationTiming>,
    range: &ReportRange,
) -> RebalanceTimings {
    let mut operations: Vec<RebalanceOperationTiming> = operations
        .into_iter()
        .filter(|operation| range.contains(operation.started_at))
        .collect();

    let stage_summary = stage_summary(&operations);

    let mut attestation_trend: Vec<AttestationSample> = operations
        .iter()
        .flat_map(|operation| &operation.stages)
        .filter(|stage| stage.stage == RebalanceStageName::Attestation && !stage.failed)
        .filter_map(|stage| {
            Some(AttestationSample {
                burned_at: stage.started_at,
                duration_ms: stage.duration_ms?,
            })
        })
        .collect();
    attestation_trend.sort_by_key(|sample| sample.burned_at);

    operations.sort_unstable_by_key(|operation| std::cmp::Reverse(operation.started_at));
    let total_operations = operations.len();
    operations.truncate(MAX_OPERATION_REPORTS);

    RebalanceTimings {
        operations,
        total_operations,
        stage_summary,
        attestation_trend,
    }
}

/// Percentiles per stage over completed, non-failed stage runs.
fn stage_summary(operations: &[RebalanceOperationTiming]) -> Vec<RebalanceStageStats> {
    use RebalanceStageName::*;

    [Conversion, Withdrawal, Burn, Attestation, Mint, Deposit]
        .into_iter()
        .filter_map(|name| {
            let samples: Vec<i64> = operations
                .iter()
                .flat_map(|operation| &operation.stages)
                .filter(|stage| stage.stage == name && !stage.failed)
                .filter_map(|stage| stage.duration_ms)
                .collect();
            Some(RebalanceStageStats {
                stage: name,
                stats: latency_stats(samples)?,
            })
        })
        .collect()
}

/// Fold one aggregate's event stream into an operation timing row.
///
/// Returns `None` for an empty stream.
fn fold_operation(
    operation_id: String,
    events: &[UsdcRebalanceEvent],
) -> Option<RebalanceOperationTiming> {
    use RebalanceStageName::*;

    let started_at = event_timestamp(events.first()?);
    let mut tracker = StageTracker::default();
    let mut direction = None;
    let mut amount = None;
    let mut status = RebalanceTimingStatus::InProgress;
    let mut completed_at = None;

    for event in events {
        match event {
            UsdcRebalanceEvent::ConversionInitiated {
                direction: event_direction,
                amount: event_amount,
                initiated_at,
                ..
            } => {
                direction.get_or_insert_with(|| event_direction.into());
                amount.get_or_insert(*event_amount);
                tracker.open(Conversion, *initiated_at);
            }
            UsdcRebalanceEvent::ConversionConfirmed {
                direction: event_direction,
                converted_at,
                ..
            } => {
                tracker.close(&operation_id, Conversion, *converted_at, false);
                if *event_direction == RebalanceDirection::BaseToAlpaca {
                    status = RebalanceTimingStatus::Completed;
                    completed_at = Some(*converted_at);
                }
            }
            UsdcRebalanceEvent::ConversionFailed { failed_at, .. } => {
                tracker.close(&operation_id, Conversion, *failed_at, true);
                status = RebalanceTimingStatus::Failed;
            }
            UsdcRebalanceEvent::WithdrawalSubmitting {
                direction: event_direction,
                amount: event_amount,
                submitting_at,
                ..
            } => {
                direction.get_or_insert_with(|| event_direction.into());
                amount.get_or_insert(*event_amount);
                tracker.open(Withdrawal, *submitting_at);
            }
            UsdcRebalanceEvent::Initiated {
                direction: event_direction,
                amount: event_amount,
                initiated_at,
                ..
            } => {
                direction.get_or_insert_with(|| event_direction.into());
                amount.get_or_insert(*event_amount);
                tracker.open_if_closed(Withdrawal, *initiated_at);
            }
            UsdcRebalanceEvent::WithdrawalConfirmed { confirmed_at } => {
                tracker.close(&operation_id, Withdrawal, *confirmed_at, false);
            }
            UsdcRebalanceEvent::WithdrawalFailed { failed_at, .. } => {
                tracker.close(&operation_id, Withdrawal, *failed_at, true);
                status = RebalanceTimingStatus::Failed;
            }
            UsdcRebalanceEvent::BridgingSubmitting { submitting_at, .. } => {
                tracker.open(Burn, *submitting_at);
            }
            UsdcRebalanceEvent::BridgingInitiated { burned_at, .. } => {
                // Streams without a BridgingSubmitting intent (direct path or
                // pre-intent history) have no measurable burn duration; a
                // synthetic zero would drag the stage percentiles down.
                if tracker.open_run_mut(Burn).is_some() {
                    tracker.close(&operation_id, Burn, *burned_at, false);
                } else {
                    tracker.record_unmeasured(Burn, *burned_at);
                }
                tracker.open(Attestation, *burned_at);
            }
            UsdcRebalanceEvent::BridgeAttestationReceived { attested_at, .. } => {
                tracker.close(&operation_id, Attestation, *attested_at, false);
                tracker.open(Mint, *attested_at);
            }
            // Recoverable stall: the attestation stage simply stays open
            // until BridgeAttestationReceived or BridgingFailed.
            UsdcRebalanceEvent::AttestationTimedOut { .. } => {}
            UsdcRebalanceEvent::Bridged { minted_at, .. } => {
                tracker.close(&operation_id, Mint, *minted_at, false);
            }
            UsdcRebalanceEvent::BridgingFailed { failed_at, .. } => {
                tracker.close_open_stages(*failed_at, true);
                status = RebalanceTimingStatus::Failed;
            }
            UsdcRebalanceEvent::BridgingCompletionRecovered { recovered_at, .. } => {
                tracker.close_open_stages(*recovered_at, false);
                // The mint demonstrably happened, but its true duration is
                // unknown (it was discovered after the fact).
                tracker.record_unmeasured(Mint, *recovered_at);
                status = RebalanceTimingStatus::InProgress;
            }
            UsdcRebalanceEvent::DepositInitiated {
                deposit_initiated_at,
                ..
            } => {
                tracker.open(Deposit, *deposit_initiated_at);
            }
            UsdcRebalanceEvent::DepositConfirmed {
                direction: event_direction,
                deposit_confirmed_at,
            } => {
                tracker.close(&operation_id, Deposit, *deposit_confirmed_at, false);
                if *event_direction == RebalanceDirection::AlpacaToBase {
                    status = RebalanceTimingStatus::Completed;
                    completed_at = Some(*deposit_confirmed_at);
                }
            }
            UsdcRebalanceEvent::DepositFailed { failed_at, .. } => {
                tracker.close(&operation_id, Deposit, *failed_at, true);
                status = RebalanceTimingStatus::Failed;
            }
            UsdcRebalanceEvent::OperatorReconciled {
                direction: event_direction,
                amount: event_amount,
                reconciled_at,
                ..
            } => {
                direction.get_or_insert_with(|| event_direction.into());
                amount.get_or_insert(*event_amount);
                // Funds were settled out-of-band: the operation is resolved
                // at reconciliation time, but no pipeline stage ran -- the
                // failed Deposit stage stays failed so percentiles ignore it.
                status = RebalanceTimingStatus::Completed;
                completed_at = Some(*reconciled_at);
            }
        }
    }

    let total_ms = completed_at.map(|at| (at - started_at).num_milliseconds());

    Some(RebalanceOperationTiming {
        operation_id,
        direction,
        amount,
        started_at,
        completed_at,
        status,
        stages: tracker.stages,
        total_ms,
    })
}

pub(super) fn event_timestamp(event: &UsdcRebalanceEvent) -> DateTime<Utc> {
    match event {
        UsdcRebalanceEvent::ConversionInitiated { initiated_at, .. }
        | UsdcRebalanceEvent::Initiated { initiated_at, .. } => *initiated_at,
        UsdcRebalanceEvent::ConversionConfirmed { converted_at, .. } => *converted_at,
        UsdcRebalanceEvent::ConversionFailed { failed_at, .. }
        | UsdcRebalanceEvent::WithdrawalFailed { failed_at, .. }
        | UsdcRebalanceEvent::BridgingFailed { failed_at, .. }
        | UsdcRebalanceEvent::DepositFailed { failed_at, .. } => *failed_at,
        UsdcRebalanceEvent::WithdrawalSubmitting { submitting_at, .. }
        | UsdcRebalanceEvent::BridgingSubmitting { submitting_at, .. } => *submitting_at,
        UsdcRebalanceEvent::WithdrawalConfirmed { confirmed_at } => *confirmed_at,
        UsdcRebalanceEvent::BridgingInitiated { burned_at, .. } => *burned_at,
        UsdcRebalanceEvent::BridgeAttestationReceived { attested_at, .. } => *attested_at,
        UsdcRebalanceEvent::AttestationTimedOut { timed_out_at, .. } => *timed_out_at,
        UsdcRebalanceEvent::Bridged { minted_at, .. } => *minted_at,
        UsdcRebalanceEvent::BridgingCompletionRecovered { recovered_at, .. } => *recovered_at,
        UsdcRebalanceEvent::DepositInitiated {
            deposit_initiated_at,
            ..
        } => *deposit_initiated_at,
        UsdcRebalanceEvent::DepositConfirmed {
            deposit_confirmed_at,
            ..
        } => *deposit_confirmed_at,
        UsdcRebalanceEvent::OperatorReconciled { reconciled_at, .. } => *reconciled_at,
    }
}

/// Builds the ordered stage list while folding an event stream.
#[derive(Debug, Default)]
struct StageTracker {
    stages: Vec<RebalanceStageTiming>,
}

impl StageTracker {
    fn open(&mut self, stage: RebalanceStageName, at: DateTime<Utc>) {
        self.stages.push(RebalanceStageTiming {
            stage,
            started_at: at,
            ended_at: None,
            duration_ms: None,
            failed: false,
        });
    }

    /// Open `stage` only if it has no currently-open run, so a redundant
    /// start marker (e.g. `Initiated` after `WithdrawalSubmitting`) does not
    /// reset the clock.
    fn open_if_closed(&mut self, stage: RebalanceStageName, at: DateTime<Utc>) {
        if self.open_run_mut(stage).is_none() {
            self.open(stage, at);
        }
    }

    fn close(
        &mut self,
        operation_id: &str,
        stage: RebalanceStageName,
        at: DateTime<Utc>,
        failed: bool,
    ) {
        let Some(run) = self.open_run_mut(stage) else {
            warn!(
                %operation_id,
                ?stage,
                "Stage end event without a matching start; skipping stage timing"
            );
            return;
        };
        run.ended_at = Some(at);
        // Clamped like the hedge pipeline samples: clock skew between event
        // sources must not push negative durations into percentiles.
        run.duration_ms = Some((at - run.started_at).num_milliseconds().max(0));
        run.failed = failed;
    }

    /// Close every open stage, e.g. when bridging fails or recovers without
    /// per-stage end markers.
    fn close_open_stages(&mut self, at: DateTime<Utc>, failed: bool) {
        for run in self.stages.iter_mut().filter(|run| run.ended_at.is_none()) {
            run.ended_at = Some(at);
            run.duration_ms = Some((at - run.started_at).num_milliseconds().max(0));
            run.failed = failed;
        }
    }

    /// Record a stage that demonstrably happened but whose duration cannot
    /// be measured from the stream; excluded from percentile summaries.
    fn record_unmeasured(&mut self, stage: RebalanceStageName, at: DateTime<Utc>) {
        self.stages.push(RebalanceStageTiming {
            stage,
            started_at: at,
            ended_at: Some(at),
            duration_ms: None,
            failed: false,
        });
    }

    fn open_run_mut(&mut self, stage: RebalanceStageName) -> Option<&mut RebalanceStageTiming> {
        self.stages
            .iter_mut()
            .rev()
            .find(|run| run.stage == stage && run.ended_at.is_none())
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{B256, TxHash};
    use chrono::TimeZone;
    use uuid::Uuid;

    use st0x_dto::UsdcBridgeDirection;
    use st0x_execution::ClientOrderId;
    use st0x_finance::Usdc;
    use st0x_float_macro::float;

    use super::*;
    use crate::test_utils::setup_test_db;
    use crate::usdc_rebalance::{ReconcileReason, TransferRef};

    fn timestamp(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_750_000_000 + seconds, 0).unwrap()
    }

    fn range() -> ReportRange {
        ReportRange {
            from: timestamp(0),
            to: timestamp(1_000_000),
        }
    }

    fn alpaca_to_base_happy_path() -> Vec<UsdcRebalanceEvent> {
        vec![
            UsdcRebalanceEvent::ConversionInitiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(1000)),
                order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
                initiated_at: timestamp(0),
            },
            UsdcRebalanceEvent::ConversionConfirmed {
                direction: RebalanceDirection::AlpacaToBase,
                filled_amount: Usdc::new(float!(1000)),
                converted_at: timestamp(10),
            },
            UsdcRebalanceEvent::WithdrawalSubmitting {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(1000)),
                from_block: 1,
                submitting_at: timestamp(20),
            },
            UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(1000)),
                withdrawal_ref: TransferRef::OnchainTx(TxHash::random()),
                initiated_at: timestamp(21),
            },
            UsdcRebalanceEvent::WithdrawalConfirmed {
                confirmed_at: timestamp(50),
            },
            UsdcRebalanceEvent::BridgingSubmitting {
                from_block: 2,
                submitting_at: timestamp(60),
            },
            UsdcRebalanceEvent::BridgingInitiated {
                burn_tx_hash: TxHash::random(),
                burned_at: timestamp(70),
            },
            UsdcRebalanceEvent::BridgeAttestationReceived {
                attestation: vec![1],
                cctp_nonce: B256::random(),
                message: None,
                mint_scan_from_block: None,
                attested_at: timestamp(670),
            },
            UsdcRebalanceEvent::Bridged {
                mint_tx_hash: TxHash::random(),
                amount_received: Usdc::new(float!(999)),
                fee_collected: Usdc::new(float!(1)),
                minted_at: timestamp(700),
            },
            UsdcRebalanceEvent::DepositInitiated {
                deposit_ref: TransferRef::OnchainTx(TxHash::random()),
                deposit_initiated_at: timestamp(710),
            },
            UsdcRebalanceEvent::DepositConfirmed {
                direction: RebalanceDirection::AlpacaToBase,
                deposit_confirmed_at: timestamp(730),
            },
        ]
    }

    fn stage(
        operation: &RebalanceOperationTiming,
        name: RebalanceStageName,
    ) -> &RebalanceStageTiming {
        operation
            .stages
            .iter()
            .find(|stage| stage.stage == name)
            .unwrap()
    }

    #[test]
    fn alpaca_to_base_completes_with_all_stage_durations() {
        let operation = fold_operation("op-1".to_string(), &alpaca_to_base_happy_path()).unwrap();

        assert_eq!(operation.status, RebalanceTimingStatus::Completed);
        assert_eq!(operation.direction, Some(UsdcBridgeDirection::AlpacaToBase));
        assert_eq!(operation.started_at, timestamp(0));
        assert_eq!(operation.completed_at, Some(timestamp(730)));
        assert_eq!(operation.total_ms, Some(730_000));

        use RebalanceStageName::*;
        assert_eq!(stage(&operation, Conversion).duration_ms, Some(10_000));
        assert_eq!(stage(&operation, Withdrawal).duration_ms, Some(30_000));
        assert_eq!(stage(&operation, Burn).duration_ms, Some(10_000));
        assert_eq!(stage(&operation, Attestation).duration_ms, Some(600_000));
        assert_eq!(stage(&operation, Mint).duration_ms, Some(30_000));
        assert_eq!(stage(&operation, Deposit).duration_ms, Some(20_000));
    }

    #[test]
    fn base_to_alpaca_completes_on_final_conversion() {
        let events = vec![
            UsdcRebalanceEvent::WithdrawalSubmitting {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(500)),
                from_block: 1,
                submitting_at: timestamp(0),
            },
            UsdcRebalanceEvent::WithdrawalConfirmed {
                confirmed_at: timestamp(30),
            },
            UsdcRebalanceEvent::DepositInitiated {
                deposit_ref: TransferRef::AlpacaId(Uuid::new_v4().into()),
                deposit_initiated_at: timestamp(40),
            },
            UsdcRebalanceEvent::DepositConfirmed {
                direction: RebalanceDirection::BaseToAlpaca,
                deposit_confirmed_at: timestamp(100),
            },
            UsdcRebalanceEvent::ConversionInitiated {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(500)),
                order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
                initiated_at: timestamp(110),
            },
            UsdcRebalanceEvent::ConversionConfirmed {
                direction: RebalanceDirection::BaseToAlpaca,
                filled_amount: Usdc::new(float!(500)),
                converted_at: timestamp(150),
            },
        ];

        let operation = fold_operation("op-2".to_string(), &events).unwrap();

        assert_eq!(operation.status, RebalanceTimingStatus::Completed);
        assert_eq!(operation.direction, Some(UsdcBridgeDirection::BaseToAlpaca));
        assert_eq!(operation.completed_at, Some(timestamp(150)));
        assert_eq!(operation.total_ms, Some(150_000));
        // The mid-flow deposit confirmation must not complete the operation.
        assert_eq!(
            stage(&operation, RebalanceStageName::Deposit).duration_ms,
            Some(60_000)
        );
    }

    #[test]
    fn withdrawal_failure_marks_operation_failed() {
        let events = vec![
            UsdcRebalanceEvent::WithdrawalSubmitting {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(500)),
                from_block: 1,
                submitting_at: timestamp(0),
            },
            UsdcRebalanceEvent::WithdrawalFailed {
                reason: "revert".to_string(),
                failed_at: timestamp(5),
            },
        ];

        let operation = fold_operation("op-3".to_string(), &events).unwrap();

        assert_eq!(operation.status, RebalanceTimingStatus::Failed);
        assert_eq!(operation.completed_at, None);
        assert_eq!(operation.total_ms, None);
        let withdrawal = stage(&operation, RebalanceStageName::Withdrawal);
        assert!(withdrawal.failed);
        assert_eq!(withdrawal.duration_ms, Some(5_000));
    }

    #[test]
    fn attestation_timeout_keeps_stage_open_until_received() {
        let events = vec![
            UsdcRebalanceEvent::BridgingInitiated {
                burn_tx_hash: TxHash::random(),
                burned_at: timestamp(0),
            },
            UsdcRebalanceEvent::AttestationTimedOut {
                burn_tx_hash: TxHash::random(),
                retry_deadline_at: timestamp(10_000),
                timed_out_at: timestamp(1_800),
            },
            UsdcRebalanceEvent::BridgeAttestationReceived {
                attestation: vec![1],
                cctp_nonce: B256::random(),
                message: None,
                mint_scan_from_block: None,
                attested_at: timestamp(3_600),
            },
        ];

        let operation = fold_operation("op-4".to_string(), &events).unwrap();

        let attestation = stage(&operation, RebalanceStageName::Attestation);
        assert_eq!(attestation.duration_ms, Some(3_600_000));
        assert!(!attestation.failed);
    }

    #[test]
    fn bridging_failure_then_recovery_returns_to_in_progress() {
        let events = vec![
            UsdcRebalanceEvent::BridgingInitiated {
                burn_tx_hash: TxHash::random(),
                burned_at: timestamp(0),
            },
            UsdcRebalanceEvent::BridgingFailed {
                burn_tx_hash: Some(TxHash::random()),
                cctp_nonce: None,
                reason: "poll exhausted".to_string(),
                failed_at: timestamp(100),
            },
            UsdcRebalanceEvent::BridgingCompletionRecovered {
                mint_tx_hash: TxHash::random(),
                amount_received: Usdc::new(float!(999)),
                fee_collected: Usdc::new(float!(1)),
                recovered_at: timestamp(200),
            },
        ];

        let operation = fold_operation("op-5".to_string(), &events).unwrap();

        assert_eq!(operation.status, RebalanceTimingStatus::InProgress);
        // Burn (unmeasured: no submitting intent), Attestation, Mint
        // (unmeasured: discovered by recovery).
        assert_eq!(operation.stages.len(), 3);
        let burn = stage(&operation, RebalanceStageName::Burn);
        assert_eq!(burn.duration_ms, None);
        assert!(!burn.failed);
        let attestation = stage(&operation, RebalanceStageName::Attestation);
        assert!(attestation.failed);
        assert_eq!(attestation.duration_ms, Some(100_000));
        // The recovered mint happened but its duration is unknowable.
        let mint = stage(&operation, RebalanceStageName::Mint);
        assert_eq!(mint.duration_ms, None);
        assert!(!mint.failed);
    }

    #[test]
    fn deposit_failure_marks_operation_failed() {
        let events = vec![
            UsdcRebalanceEvent::DepositInitiated {
                deposit_ref: TransferRef::OnchainTx(TxHash::random()),
                deposit_initiated_at: timestamp(0),
            },
            UsdcRebalanceEvent::DepositFailed {
                deposit_ref: None,
                reason: "deposit revert".to_string(),
                failed_at: timestamp(20),
            },
        ];

        let operation = fold_operation("op-6".to_string(), &events).unwrap();

        assert_eq!(operation.status, RebalanceTimingStatus::Failed);
        let deposit = stage(&operation, RebalanceStageName::Deposit);
        assert!(deposit.failed);
        assert_eq!(deposit.duration_ms, Some(20_000));
    }

    #[test]
    fn operator_reconcile_completes_stranded_deposit_failure() {
        let events = vec![
            UsdcRebalanceEvent::DepositInitiated {
                deposit_ref: TransferRef::OnchainTx(TxHash::random()),
                deposit_initiated_at: timestamp(0),
            },
            UsdcRebalanceEvent::DepositFailed {
                deposit_ref: None,
                reason: "deposit revert".to_string(),
                failed_at: timestamp(20),
            },
            UsdcRebalanceEvent::OperatorReconciled {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(1000)),
                reason: ReconcileReason::FundsMovedManually,
                initiated_at: timestamp(0),
                reconciled_at: timestamp(500),
            },
        ];

        let operation = fold_operation("op-7".to_string(), &events).unwrap();

        // The operation is resolved, but the failed Deposit stage keeps its
        // failure so stage percentiles never absorb a manual recovery.
        assert_eq!(operation.status, RebalanceTimingStatus::Completed);
        assert_eq!(operation.completed_at, Some(timestamp(500)));
        assert_eq!(operation.total_ms, Some(500_000));
        assert_eq!(
            operation.direction,
            Some(st0x_dto::UsdcBridgeDirection::AlpacaToBase)
        );
        let deposit = stage(&operation, RebalanceStageName::Deposit);
        assert!(deposit.failed);
    }

    #[test]
    fn conversion_failure_marks_operation_failed() {
        let events = vec![
            UsdcRebalanceEvent::ConversionInitiated {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(500)),
                order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
                initiated_at: timestamp(0),
            },
            UsdcRebalanceEvent::ConversionFailed {
                reason: "order rejected".to_string(),
                failed_at: timestamp(8),
            },
        ];

        let operation = fold_operation("op-7".to_string(), &events).unwrap();

        assert_eq!(operation.status, RebalanceTimingStatus::Failed);
        let conversion = stage(&operation, RebalanceStageName::Conversion);
        assert!(conversion.failed);
        assert_eq!(conversion.duration_ms, Some(8_000));
    }

    #[test]
    fn negative_stage_durations_clamp_to_zero() {
        // Clock skew: the confirmation timestamp precedes the submission.
        let events = vec![
            UsdcRebalanceEvent::WithdrawalSubmitting {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(500)),
                from_block: 1,
                submitting_at: timestamp(10),
            },
            UsdcRebalanceEvent::WithdrawalConfirmed {
                confirmed_at: timestamp(5),
            },
        ];

        let operation = fold_operation("op-8".to_string(), &events).unwrap();

        let withdrawal = stage(&operation, RebalanceStageName::Withdrawal);
        assert_eq!(withdrawal.duration_ms, Some(0));
    }

    #[test]
    fn report_caps_operation_rows_but_counts_all() {
        let operations: Vec<RebalanceOperationTiming> = (0..=i64::try_from(MAX_OPERATION_REPORTS)
            .unwrap())
            .map(|index| {
                fold_operation(
                    format!("op-{index}"),
                    &[UsdcRebalanceEvent::WithdrawalSubmitting {
                        direction: RebalanceDirection::BaseToAlpaca,
                        amount: Usdc::new(float!(1)),
                        from_block: 1,
                        submitting_at: timestamp(index),
                    }],
                )
                .unwrap()
            })
            .collect();

        let report = rebalance_timing_report(operations, &range());

        assert_eq!(report.total_operations, MAX_OPERATION_REPORTS + 1);
        assert_eq!(report.operations.len(), MAX_OPERATION_REPORTS);
    }

    #[test]
    fn report_summarizes_stages_and_attestation_trend() {
        let first = fold_operation("op-1".to_string(), &alpaca_to_base_happy_path()).unwrap();

        let report = rebalance_timing_report(vec![first], &range());

        assert_eq!(report.total_operations, 1);
        assert_eq!(report.operations.len(), 1);
        assert_eq!(report.attestation_trend.len(), 1);
        assert_eq!(report.attestation_trend[0].burned_at, timestamp(70));
        assert_eq!(report.attestation_trend[0].duration_ms, 600_000);

        let attestation_stats = report
            .stage_summary
            .iter()
            .find(|entry| entry.stage == RebalanceStageName::Attestation)
            .unwrap();
        assert_eq!(attestation_stats.stats.p50_ms, 600_000);
        assert_eq!(attestation_stats.stats.sample_count, 1);
    }

    #[test]
    fn report_excludes_operations_outside_range() {
        let operation = fold_operation("op-1".to_string(), &alpaca_to_base_happy_path()).unwrap();
        let narrow = ReportRange {
            from: timestamp(100_000),
            to: timestamp(200_000),
        };

        let report = rebalance_timing_report(vec![operation], &narrow);

        assert_eq!(report.total_operations, 0);
        assert!(report.operations.is_empty());
        assert!(report.stage_summary.is_empty());
        assert!(report.attestation_trend.is_empty());
    }

    #[tokio::test]
    async fn load_rebalance_timings_reads_event_store() {
        let pool = setup_test_db().await;
        let operation_id = Uuid::new_v4().to_string();

        for (sequence, event) in alpaca_to_base_happy_path().iter().enumerate() {
            let payload = serde_json::to_string(event).unwrap();
            sqlx::query(
                "INSERT INTO events \
                 (aggregate_type, aggregate_id, sequence, event_type, event_version, \
                  payload, metadata) \
                 VALUES ('UsdcRebalance', $1, $2, $3, '1.0', $4, '{}')",
            )
            .bind(&operation_id)
            .bind(i64::try_from(sequence).unwrap() + 1)
            .bind(format!("event-{sequence}"))
            .bind(payload)
            .execute(&pool)
            .await
            .unwrap();
        }

        let report = load_rebalance_timings(&pool, &range()).await.unwrap();

        assert_eq!(report.total_operations, 1);
        let operation = &report.operations[0];
        assert_eq!(operation.operation_id, operation_id);
        assert_eq!(operation.status, RebalanceTimingStatus::Completed);
        // Stage timings must survive the serialize -> DB -> fold round trip.
        assert_eq!(operation.total_ms, Some(730_000));
        assert_eq!(
            stage(operation, RebalanceStageName::Attestation).duration_ms,
            Some(600_000)
        );
        assert_eq!(
            stage(operation, RebalanceStageName::Deposit).duration_ms,
            Some(20_000)
        );
    }
}

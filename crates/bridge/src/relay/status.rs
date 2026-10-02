//! `GET /intents/status/v3`: where Relay says an order is.

use alloy::primitives::TxHash;
use serde::Deserialize;

/// One status read: Relay's status plus every tx it has seen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntentStatusReport {
    pub status: IntentStatus,
    /// Relay's `inTxHashes`: the origin-chain deposits it attributes to the
    /// request.
    pub deposit_txs: Vec<TxHash>,
    /// Relay's `txHashes`, kept whatever the status: the fills on `Success`,
    /// the refunds on `Refund`, and any tx Relay lists on the others (one may
    /// still confirm after `TRANSACTION_NOT_INCLUDED`).
    pub txs: Vec<TxHash>,
}

/// Relay's status of a request. Only `Success`, `Refund`, `RefundFailed` and
/// `Failure` are terminal; a status this build does not know keeps the
/// transfer waiting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntentStatus {
    /// Quoted, no deposit seen.
    Waiting,
    InFlight(InFlightStage),
    /// Filled: [`IntentStatusReport::txs`] is never empty and settlement
    /// proof checks each.
    Success,
    /// Relay says `success` but names no fill tx yet.
    Filling,
    /// Paid back to `refund_to`: on the origin chain in the origin stable or
    /// on the destination chain in the destination stable, as the quote
    /// offers both. [`IntentStatusReport::txs`] is never empty and Relay
    /// reports no refund fail reason.
    Refund {
        reason: Option<FailReason>,
    },
    /// Relay says `refund` but names no refund tx yet.
    Refunding {
        reason: Option<FailReason>,
    },
    /// Relay says `refund` with a refund fail reason: the refund will not be
    /// paid. The fail reason wins over any refund tx Relay still lists.
    RefundFailed {
        reason: Option<FailReason>,
        /// Relay's `refundFailReason`: why the refund could not be paid.
        refund_fail_reason: FailReason,
    },
    Failure {
        reason: Option<FailReason>,
    },
    /// A status name this build does not know, kept verbatim.
    Unknown(String),
}

impl IntentStatus {
    pub const fn is_terminal(&self) -> bool {
        match self {
            Self::Success
            | Self::Refund { .. }
            | Self::RefundFailed { .. }
            | Self::Failure { .. } => true,
            Self::Waiting
            | Self::InFlight(_)
            | Self::Filling
            | Self::Refunding { .. }
            | Self::Unknown(_) => false,
        }
    }
}

/// The non-terminal statuses Relay reports after the deposit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InFlightStage {
    Depositing,
    Pending,
    Submitted,
    /// Undocumented beyond "still processing".
    Delayed,
}

/// Relay's `failReason`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailReason {
    Slippage,
    TooLittleReceived,
    SolverCapacityExceeded,
    TtlExpired,
    DepositConfirmationTimeout,
    DepositReorged,
    BlockedWallet,
    TransactionNotIncluded,
    /// The deposit was smaller than the quoted amount.
    DepositedAmountTooLowToFill,
    /// A code this build does not know, kept verbatim.
    Unknown(String),
}

impl FailReason {
    /// `None` for Relay's `"N/A"`.
    fn parse(code: &str) -> Option<Self> {
        Some(match code {
            "N/A" => return None,
            "SLIPPAGE" => Self::Slippage,
            "TOO_LITTLE_RECEIVED" => Self::TooLittleReceived,
            "SOLVER_CAPACITY_EXCEEDED" => Self::SolverCapacityExceeded,
            "TTL_EXPIRED" => Self::TtlExpired,
            "DEPOSIT_CONFIRMATION_TIMEOUT" => Self::DepositConfirmationTimeout,
            "DEPOSIT_REORGED" => Self::DepositReorged,
            "BLOCKED_WALLET" => Self::BlockedWallet,
            "TRANSACTION_NOT_INCLUDED" => Self::TransactionNotIncluded,
            "DEPOSITED_AMOUNT_TOO_LOW_TO_FILL" => Self::DepositedAmountTooLowToFill,
            other => Self::Unknown(other.to_owned()),
        })
    }
}

/// Wire shape. A never-deposited request carries only `status` and
/// `quoteCreatedAt`, so the arrays default to empty.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct StatusResponse {
    status: String,
    #[serde(default)]
    in_tx_hashes: Vec<TxHash>,
    #[serde(default)]
    tx_hashes: Vec<TxHash>,
    fail_reason: Option<String>,
    refund_fail_reason: Option<String>,
}

impl From<StatusResponse> for IntentStatusReport {
    fn from(response: StatusResponse) -> Self {
        let reason = response.fail_reason.as_deref().and_then(FailReason::parse);
        let refund_fail_reason = response
            .refund_fail_reason
            .as_deref()
            .and_then(FailReason::parse);

        let status = match response.status.as_str() {
            "waiting" => IntentStatus::Waiting,
            "depositing" => IntentStatus::InFlight(InFlightStage::Depositing),
            "pending" => IntentStatus::InFlight(InFlightStage::Pending),
            "submitted" => IntentStatus::InFlight(InFlightStage::Submitted),
            "delayed" => IntentStatus::InFlight(InFlightStage::Delayed),
            "success" if response.tx_hashes.is_empty() => IntentStatus::Filling,
            "success" => IntentStatus::Success,
            "refund" => match refund_fail_reason {
                Some(refund_fail_reason) => IntentStatus::RefundFailed {
                    reason,
                    refund_fail_reason,
                },
                None if response.tx_hashes.is_empty() => IntentStatus::Refunding { reason },
                None => IntentStatus::Refund { reason },
            },
            "failure" => IntentStatus::Failure { reason },
            _ => IntentStatus::Unknown(response.status),
        };

        Self {
            status,
            deposit_txs: response.in_tx_hashes,
            txs: response.tx_hashes,
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::b256;
    use serde_json::json;

    use super::*;

    fn report(body: &str) -> IntentStatusReport {
        serde_json::from_str::<StatusResponse>(body).unwrap().into()
    }

    #[test]
    fn waiting_status_has_no_txs() {
        let report = report(include_str!("../../relay-fixtures/status_waiting.json"));

        assert_eq!(
            report,
            IntentStatusReport {
                status: IntentStatus::Waiting,
                deposit_txs: vec![],
                txs: vec![],
            }
        );
        assert!(!report.status.is_terminal());
    }

    #[test]
    fn pending_status_is_in_flight_with_the_deposit() {
        let report = report(include_str!("../../relay-fixtures/status_pending.json"));

        assert_eq!(
            report,
            IntentStatusReport {
                status: IntentStatus::InFlight(InFlightStage::Pending),
                deposit_txs: vec![b256!(
                    "0xeeee66456ace7aae93e6ed814d32a3748a5fc86d7101a259a4f62a44822c819d"
                )],
                txs: vec![],
            }
        );
        assert!(!report.status.is_terminal());
    }

    #[test]
    fn success_status_names_the_fill_tx() {
        let report = report(include_str!("../../relay-fixtures/status_success.json"));

        assert_eq!(report.status, IntentStatus::Success);
        assert_eq!(
            report.txs,
            vec![b256!(
                "0x4d08b9f1596e351ac0b9ea38702a83c28a53b20fa3fdd9ada60d2a0b556bc40d"
            )]
        );
        assert_eq!(
            report.deposit_txs,
            vec![b256!(
                "0x8b4dba3c5bc03dd67671c71836b4ab61c2435c7bf2dacc11ab80362225fce961"
            )]
        );
        assert!(report.status.is_terminal());
    }

    #[test]
    fn success_without_a_fill_tx_is_not_terminal() {
        let report = report(&json!({"status": "success", "failReason": "N/A"}).to_string());

        assert_eq!(report.status, IntentStatus::Filling);
        assert!(!report.status.is_terminal());
    }

    #[test]
    fn refund_status_names_refund_tx_and_fail_reason() {
        let report = report(include_str!("../../relay-fixtures/status_refund.json"));

        assert_eq!(
            report.status,
            IntentStatus::Refund {
                reason: Some(FailReason::DepositedAmountTooLowToFill),
            }
        );
        assert_eq!(
            report.txs,
            vec![b256!(
                "0xf27f49b3e941788a37775b874e1a91a711c26578c041921a24efd96cd14cea8d"
            )]
        );
        assert_eq!(
            report.deposit_txs,
            vec![b256!(
                "0x1dd1b32e03951ea87347dd8234b120b50f16443d8085bb161539e212649b8d81"
            )]
        );
        assert!(report.status.is_terminal());
    }

    #[test]
    fn refund_without_a_refund_tx_is_still_refunding() {
        let report = report(
            &json!({
                "status": "refund",
                "txHashes": [],
                "failReason": "SLIPPAGE",
                "refundFailReason": "N/A",
            })
            .to_string(),
        );

        assert_eq!(
            report.status,
            IntentStatus::Refunding {
                reason: Some(FailReason::Slippage),
            }
        );
        assert!(!report.status.is_terminal());
    }

    #[test]
    fn refund_with_a_refund_fail_reason_is_a_failed_refund() {
        let report = report(
            &json!({
                "status": "refund",
                "txHashes": [
                    "0xf27f49b3e941788a37775b874e1a91a711c26578c041921a24efd96cd14cea8d",
                ],
                "failReason": "SLIPPAGE",
                "refundFailReason": "BLOCKED_WALLET",
            })
            .to_string(),
        );

        assert_eq!(
            report.status,
            IntentStatus::RefundFailed {
                reason: Some(FailReason::Slippage),
                refund_fail_reason: FailReason::BlockedWallet,
            }
        );
        assert_eq!(
            report.txs,
            vec![b256!(
                "0xf27f49b3e941788a37775b874e1a91a711c26578c041921a24efd96cd14cea8d"
            )]
        );
        assert!(report.status.is_terminal());
    }

    #[test]
    fn unknown_status_is_non_terminal() {
        let report = report(&json!({"status": "teleporting", "quoteCreatedAt": 1}).to_string());

        assert_eq!(
            report.status,
            IntentStatus::Unknown("teleporting".to_owned())
        );
        assert!(!report.status.is_terminal());
    }

    #[test]
    fn every_tx_hash_is_kept() {
        let report = report(
            &json!({
                "status": "success",
                "inTxHashes": [
                    "0x0000000000000000000000000000000000000000000000000000000000000001",
                    "0x0000000000000000000000000000000000000000000000000000000000000002",
                ],
                "txHashes": [
                    "0x0000000000000000000000000000000000000000000000000000000000000003",
                    "0x0000000000000000000000000000000000000000000000000000000000000004",
                ],
                "failReason": "N/A",
            })
            .to_string(),
        );

        assert_eq!(
            report,
            IntentStatusReport {
                status: IntentStatus::Success,
                deposit_txs: vec![
                    b256!("0x0000000000000000000000000000000000000000000000000000000000000001"),
                    b256!("0x0000000000000000000000000000000000000000000000000000000000000002"),
                ],
                txs: vec![
                    b256!("0x0000000000000000000000000000000000000000000000000000000000000003"),
                    b256!("0x0000000000000000000000000000000000000000000000000000000000000004"),
                ],
            }
        );
    }

    #[test]
    fn failure_keeps_an_unknown_reason_verbatim() {
        let report =
            report(&json!({"status": "failure", "failReason": "SOMETHING_NEW"}).to_string());

        assert_eq!(
            report.status,
            IntentStatus::Failure {
                reason: Some(FailReason::Unknown("SOMETHING_NEW".to_owned())),
            }
        );
        assert!(report.status.is_terminal());
    }

    #[test]
    fn failure_keeps_the_listed_tx() {
        let report = report(
            &json!({
                "status": "failure",
                "txHashes": [
                    "0x0000000000000000000000000000000000000000000000000000000000000005",
                ],
                "failReason": "TRANSACTION_NOT_INCLUDED",
            })
            .to_string(),
        );

        assert_eq!(
            report,
            IntentStatusReport {
                status: IntentStatus::Failure {
                    reason: Some(FailReason::TransactionNotIncluded),
                },
                deposit_txs: vec![],
                txs: vec![b256!(
                    "0x0000000000000000000000000000000000000000000000000000000000000005"
                )],
            }
        );
    }

    #[test]
    fn in_flight_and_unknown_statuses_keep_the_listed_tx() {
        for (status, expected) in [
            (
                "submitted",
                IntentStatus::InFlight(InFlightStage::Submitted),
            ),
            (
                "teleporting",
                IntentStatus::Unknown("teleporting".to_owned()),
            ),
        ] {
            let report = report(
                &json!({
                    "status": status,
                    "txHashes": [
                        "0x0000000000000000000000000000000000000000000000000000000000000006",
                    ],
                })
                .to_string(),
            );

            assert_eq!(report.status, expected);
            assert_eq!(
                report.txs,
                vec![b256!(
                    "0x0000000000000000000000000000000000000000000000000000000000000006"
                )]
            );
        }
    }
}

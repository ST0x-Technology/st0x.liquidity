//! Operational alerting: out-of-band notifications for conditions an operator
//! must react to (low native-gas balance, stuck rebalancing transfers,
//! dead-lettered hedges, supervised-worker terminal failures).
//!
//! The [`Notifier`] trait abstracts the delivery channel; [`LogNotifier`] is
//! the only production implementation: it emits each alert as a structured
//! ERROR log with target `operational_alert`. Delivery to humans happens
//! downstream, in the log pipeline (Cloud Logging -> Grafana alert rules,
//! matching on the target string in the gcplogs stream), so the bot itself
//! holds no delivery credentials and delivery cannot fail in-process.
//!
//! Monitors that raise alerts (see `crate::conductor::monitor::gas`) depend on
//! the trait so they stay testable against a capturing mock.

use async_trait::async_trait;
use std::cmp::Reverse;
use tracing::error;

/// Declares [`AlertKind`] from its two vocabularies, so the enum, its strings
/// and the ordered lists cannot drift apart.
///
/// `extracted` lists, in order, the phrases the `operational_alerts` kind
/// extractor matches in the message text (t0.devops
/// `modules/observability-consumer`, mirrored verbatim by the unclassified
/// rule in `observability/alerting/liquidity.rules.yml`). The order is the
/// extractor's alternation order, which decides a tie between two phrases
/// starting at the same position. `not_extracted` lists kinds of alerts the
/// extractor has no phrase for: they page as unclassified until the extractor
/// and the rules learn them.
macro_rules! alert_kinds {
    (
        extracted { $($extracted:ident => $extracted_str:literal,)* }
        not_extracted { $($new:ident => $new_str:literal,)* }
    ) => {
        /// The class of an operational alert, logged as the `kind` field of
        /// the `operational_alert` line (`jsonPayload.kind` once the console
        /// logs JSON).
        ///
        /// Each extracted kind's string is exactly the label the
        /// `operational_alerts` extractor gives the same line once it carries
        /// the rules file's full phrase list, so the Grafana rules keyed on
        /// `kind` match the field unchanged.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub(crate) enum AlertKind {
            $($extracted,)*
            $($new,)*
        }

        impl AlertKind {
            pub(crate) const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$extracted => $extracted_str,)*
                    $(Self::$new => $new_str,)*
                }
            }
        }

        /// The kinds the extractor knows, in its alternation order.
        const EXTRACTED: &[AlertKind] = &[$(AlertKind::$extracted,)*];

        #[cfg(test)]
        const NOT_EXTRACTED: &[AlertKind] = &[$(AlertKind::$new,)*];
    };
}

alert_kinds! {
    extracted {
        LowGas => "Low gas",
        GasRecovered => "Gas recovered",
        PortfolioSnapshotMarkStale => "Portfolio snapshot mark stale",
        PortfolioSnapshotMarkMissing => "Portfolio snapshot mark missing",
        InventoryVaultUnderFunded => "inventory vault under-funded",
        RaindexVaultWithdrawFailed => "Raindex vault withdraw failed",
        JobFailedAfterRetries => "Job failed after retries",
        ExceedsNominal => "exceeds nominal",
        BurnSubmissionInconclusive => "burn submission inconclusive",
        UsdcLatchedOnStartup => "USDC rebalancing is LATCHED on startup with no automated recovery",
        TimedOutAfterCctpBurn => "timed out after CCTP burn",
        MarketMakerWalletAlreadyHolds => "market-maker wallet already holds",
        FillOnDisabledAsset => "Fill on DISABLED asset",
        UsdcShortfallCheckOff => "the USDC shortfall check is off",
        ShortOfOpenUsdcTransferCredits => "short of the credits of the open USDC transfers",
        CreditedLessUsdcThanRequested => "credited less USDC than requested",
        CctpMintUnresolvable => "the CCTP mint cannot be resolved automatically",
        BurnedUsdcUnmintable => "the burned USDC cannot be minted automatically",
        CompleteWithNoTxHash => "complete with no tx hash",
        AttestedCctpMessageNonceMismatch => "the attested CCTP message does not match the recorded nonce",
        RecordedCctpMessageCannotMint => "the recorded CCTP message cannot mint on Base",
        CctpMintOnBaseIncomplete => "the CCTP mint on Base did not complete",
        SharedAlpacaWithdrawalTx => "share one Alpaca withdrawal tx",
        AlpacaDepositSendPersistenceUnknown => "Cannot tell whether a signed Alpaca deposit send was persisted",
        AlpacaDepositSendsUnlisted => "Could not list signed Alpaca deposit sends at startup",
        AlpacaDepositSendIdsUnparseable => "Signed Alpaca deposit sends with unparseable transfer ids were not restored",
        AlpacaDepositSendTransferUnloadable => "Could not load a transfer with a signed Alpaca deposit send",
        AlpacaDepositSendRebroadcastFailed => "Could not rebroadcast a signed Alpaca deposit send at startup",
        RebroadcastFailedAtStartup => "could not be rebroadcast at startup",
        DepositSendQueuesEthereumNonce => "later sends from the Ethereum wallet queue behind its nonce",
        DepositMarkedFailed => "deposit marked failed for operator reconciliation",
        WithdrawalTxAlreadyRecorded => "is already recorded by USDC rebalance",
        UsdcCorridorMismatch => "USDC transfer corridor mismatch",
        UsdcLatchedOnEveryCorridor => "USDC rebalancing is LATCHED on every corridor with no automated recovery",
        UsdcBlockedOnEveryCorridor => "USDC rebalancing is BLOCKED on every corridor",
        HedgeOrderGateCorrected => "against durable Position state",
        WithdrawalHashUnavailableAtStartup => "whose hash the node cannot return at startup",
        RaindexVaultWithdrawalUnconfirmed => "Raindex vault withdrawal has stayed unconfirmed",
        TransferJobBudgetExhausted => "exhausted its transfer job budget",
        MintCompletesOnceIssuanceAccepts => "completes automatically once issuance accepts",
        MintAcceptedUntilIssuanceResolves => "stays in MintAccepted until resolved on the issuance side",
        TransferTimeoutUncertainOutcome => "exceeded the transfer timeout with an uncertain provider outcome",
        StandingDelta => "carries a standing delta",
        ResidualExposureAfterLatchedClose => "Residual exposure remains after the latched close",
        StuckAtCctpMintRecovery => "currently stuck at CCTP mint recovery",
        StructurallyDeadAlpacaIntegration => "structurally-dead Alpaca integration",
        ConsecutiveReschedules => "consecutive reschedules",
        WithdrawalPollingInconclusive => "withdrawal polling inconclusive",
        AttestationRetryDeadlineElapsed => "attestation retry deadline elapsed",
        ClassifiedDropped => "classified dropped",
        NoLongerInMempool => "no longer in the mempool",
        NotDurablyRecorded => "could not be durably recorded",
        RecordPendingBurnUncommitted => "RecordPendingBurn could not be committed",
        BurnSubmitTaskPanicked => "burn submit-and-record task panicked",
        ConversionLatchedBeforeForcing => "before forcing this rebalance either way",
        NoWithdrawalAttempted => "No withdrawal was attempted",
        PerAttemptTimeoutRedriveLimit => "per-attempt timeout redrive limit reached",
        PerAttemptTimeoutRetried => "per-attempt timeout has retried",
        BurnRevertRedriveLimit => "burn revert redrive limit reached",
        BurnRevertRetried => "burn revert has retried",
        VaultWithdrawalScanInconclusive => "vault-withdrawal scan has stayed inconclusive",
        SettlementRetryDeadlineElapsed => "settlement retry deadline elapsed",
        WithdrawalCreditMismatch => "to the market-maker wallet against nominal",
        WithdrawalCreditUncomputable => "USDC credit of withdrawal tx",
        NoRecordedWithdrawalTxHash => "no recorded withdrawal tx hash",
        DashboardTradeDeliveryFailed => "Dashboard trade delivery failed after retries",
        MintAuthorizationDeliveryFailed => "Mint authorization delivery failed all retries",
        TokenizationResumeFailed => "Interrupted tokenization aggregate failed all resume retries",
    }
    not_extracted {
        PnlLedgerCatchUpFailed => "PnL ledger failed to catch up at startup",
        RedemptionLegacyPendingSend => "has a legacy pending send to the issuer",
        RedemptionSendSignedByAnotherWallet => "not the wallet that sends to the issuer",
        RedemptionSendUnresolved => "recovery will rebroadcast the same transaction",
        HedgeStalledScanNotRunning => "Hedge stalled: scan not running",
        HedgeStalledSessionUnreadable => "Hedge stalled: market session unreadable",
        HedgeStalledExposureNotPlaced => "Hedge stalled: exposure not placed",
        HedgeStalledOrderNotCompleting => "Hedge stalled: order not completing",
        HedgeStalledAnchoredTooLong => "Hedge stalled: anchored too long",
        RedemptionLegacyUnderlyingMismatch => "its legacy record names the underlying",
        RecoveryHoldsSymbol => "Pausing the listing does not end the hold",
        UsdcHeldWithoutConfirmedBurn => "with no confirmed CCTP burn",
        RedemptionSendTimedOut => "has a signed redemption transfer to the issuer",
        CctpBurnEmittedNoMessageSent => "CCTP burn mined and succeeded but emitted no MessageSent",
        CapitalCctpBurnsUnlisted => "Could not list the pending capital CCTP burns at startup",
        CapitalCctpBurnIdsUnparseable => "Pending capital CCTP burns with unparseable operation ids were not restored",
        CapitalCctpBurnUnloadable => "Could not load a pending capital CCTP burn at startup",
        CapitalCctpBurnSignedByAnotherWallet => "A pending capital CCTP burn was signed by another wallet",
        CapitalCctpBurnRebroadcastFailed => "Could not rebroadcast a pending capital CCTP burn at startup",
        CapitalCctpBridgeUnavailable => "Could not build the CCTP bridge to restore pending capital CCTP burns",
        CapitalCctpBurnSendUnproven => "A recorded CCTP burn could not be sent",
        CapitalCctpBurnRecordUnknown => "Cannot tell whether a signed CCTP burn was recorded",
        RecoveryActiveTransferOnOtherChain => "the symbol's active transfer is on another chain",
        RecoveryChainServicesMissing => "its chain has no equity services",
        WrappedRecoveryValidationFailed => "active aggregate validation failed",
        RecoveryClaimHeldForOtherChain => "the slot is held for another chain's recovery",
    }
}

impl AlertKind {
    /// The kind the `operational_alerts` extractor gives `text`: the extracted
    /// phrase that starts rightmost on the first line holding any, ties going
    /// to the earlier phrase in the extractor's order. This is what the
    /// extractor's `.*(phrase|...)` regex captures, since `.` stops at a
    /// newline and the greedy `.*` takes the rightmost match.
    ///
    /// Wrapper alerts, whose text embeds an error rendered from deep inside a
    /// job, take their kind from this, so the most specific cause wins
    /// exactly as it does in the extractor.
    pub(crate) fn most_specific_in(text: &str) -> Option<Self> {
        text.split('\n').find_map(|line| {
            EXTRACTED
                .iter()
                .enumerate()
                .filter_map(|(order, kind)| {
                    line.rfind(kind.as_str())
                        .map(|start| (start, Reverse(order), *kind))
                })
                .max_by_key(|(start, order, _)| (*start, *order))
                .map(|(_, _, kind)| kind)
        })
    }
}

/// Sends an operational alert over some channel.
///
/// Kept as a trait so monitors depend on the capability, not the concrete
/// log transport, which keeps them unit-testable with a capturing mock.
#[async_trait]
pub(crate) trait Notifier: Send + Sync {
    async fn notify(&self, kind: AlertKind, message: &str) -> Result<(), NotifierError>;
}

/// Error type of [`Notifier::notify`].
///
/// Uninhabited outside test builds: the production [`LogNotifier`] emits a
/// log line and cannot fail, so `notify` is infallible in practice. The
/// `Result` stays in the trait so alert-failure handling at the call sites
/// (retry/backoff paths, bounded-timeout sends) remains exercisable in tests.
#[derive(Debug, thiserror::Error)]
pub(crate) enum NotifierError {
    /// Simulated delivery failure, constructible only from tests, for
    /// exercising the call sites' alert-failure handling.
    #[cfg(test)]
    #[error("simulated notifier delivery failure")]
    Simulated,
}

/// A [`Notifier`] that emits each alert as a structured ERROR log.
///
/// The target string `operational_alert` is the delivery contract: the
/// downstream metric filter matches it as a substring of the log entry. The
/// `alert = true` field is a secondary marker for structured queries, `kind`
/// is the alert class, and the human-readable alert text is the event message
/// (the `message` field in JSON log output).
pub(crate) struct LogNotifier;

#[async_trait]
impl Notifier for LogNotifier {
    async fn notify(&self, kind: AlertKind, message: &str) -> Result<(), NotifierError> {
        error!(target: "operational_alert", alert = true, kind = kind.as_str(), "{message}");
        Ok(())
    }
}

#[cfg(test)]
pub(crate) use test_support::CapturingNotifier;

/// Test-only notifier helpers. Lives in a `#[cfg(test)]` module (rather than
/// bare `#[cfg(test)]` items) so clippy's `allow-unwrap-in-tests` applies to the
/// `Mutex`-lock unwraps below, matching the crate's `test_utils` pattern.
#[cfg(test)]
mod test_support {
    use async_trait::async_trait;

    use super::{AlertKind, EXTRACTED, Notifier, NotifierError};

    /// A [`Notifier`] that captures every alert passed to `notify()`, for tests
    /// that assert operator alerts fire at the right moments without asserting
    /// on log output. Shared across the crate's test modules.
    ///
    /// Every capture also checks the kind against the extractor: an extracted
    /// kind must be what the extractor reads from the message, and a kind the
    /// extractor does not know must not collide with one it does. So every
    /// test that drives an alert path pins its call site's kind.
    #[derive(Default)]
    pub(crate) struct CapturingNotifier {
        captured: std::sync::Mutex<Vec<(AlertKind, String)>>,
    }

    impl CapturingNotifier {
        pub(crate) fn messages(&self) -> Vec<String> {
            self.captured
                .lock()
                .unwrap()
                .iter()
                .map(|(_, message)| message.clone())
                .collect()
        }

        pub(crate) fn kinds(&self) -> Vec<AlertKind> {
            self.captured
                .lock()
                .unwrap()
                .iter()
                .map(|(kind, _)| *kind)
                .collect()
        }
    }

    #[async_trait]
    impl Notifier for CapturingNotifier {
        async fn notify(&self, kind: AlertKind, message: &str) -> Result<(), NotifierError> {
            assert_kind_matches_extractor(kind, message);
            self.captured
                .lock()
                .unwrap()
                .push((kind, message.to_string()));
            Ok(())
        }
    }

    /// Fails when `kind` is not the label the extractor gives `message`: the
    /// extracted phrase itself, or, for a kind the extractor does not know,
    /// no phrase at all.
    pub(crate) fn assert_kind_matches_extractor(kind: AlertKind, message: &str) {
        let expected = EXTRACTED.contains(&kind).then_some(kind);
        assert_eq!(
            AlertKind::most_specific_in(message),
            expected,
            "alert kind {kind:?} disagrees with the extractor on {message:?}"
        );
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use regex::Regex;

    use super::*;

    /// The extractor's alternation, verbatim from the unclassified rule in
    /// the rules file (which mirrors t0.devops `observability-consumer`).
    fn rules_file_kinds() -> Vec<String> {
        let rules = include_str!("../../observability/alerting/liquidity.rules.yml");
        let alternation = rules
            .lines()
            .skip_while(|line| line.trim() != r#"- "!=~""#)
            .nth(1)
            .expect("the unclassified rule lists the extractor's kinds after !=~")
            .trim()
            .trim_start_matches(r#"- ""#)
            .trim_end_matches('"');
        alternation.split('|').map(str::to_string).collect()
    }

    /// The extractor regex itself, as Cloud Logging runs it (RE2, which
    /// shares the `regex` crate's leftmost-first semantics).
    fn extractor_regex() -> Regex {
        Regex::new(&format!(".*({})", rules_file_kinds().join("|"))).unwrap()
    }

    #[test]
    fn extracted_kinds_are_exactly_the_extractors_list_in_its_order() {
        let ours: Vec<&str> = EXTRACTED.iter().map(|kind| kind.as_str()).collect();

        assert_eq!(ours, rules_file_kinds());
    }

    /// Every per-kind rule in the rules file names a kind the bot emits or
    /// the extractor knows, so a typo on either side fails here.
    #[test]
    fn every_rule_kind_is_an_extracted_kind() {
        let rules = include_str!("../../observability/alerting/liquidity.rules.yml");
        let lines: Vec<&str> = rules.lines().collect();
        let rule_kinds: Vec<&str> = lines
            .windows(3)
            .filter(|window| {
                window[0].trim() == r#"- "metric.labels.kind""# && window[1].trim() == r#"- "=""#
            })
            .map(|window| {
                window[2]
                    .trim()
                    .trim_start_matches(r#"- ""#)
                    .trim_end_matches('"')
            })
            .collect();

        assert!(
            rule_kinds.len() > 60,
            "found {} per-kind rules",
            rule_kinds.len()
        );
        for kind in rule_kinds {
            assert!(
                EXTRACTED.iter().any(|extracted| extracted.as_str() == kind),
                "rule kind {kind:?} is not an extracted AlertKind"
            );
        }
    }

    #[test]
    fn kinds_the_extractor_does_not_know_contain_no_extracted_phrase() {
        for kind in NOT_EXTRACTED {
            assert_eq!(
                AlertKind::most_specific_in(kind.as_str()),
                None,
                "{kind:?} would read as an extracted kind in a text log line"
            );
            assert!(
                !EXTRACTED
                    .iter()
                    .any(|extracted| extracted.as_str() == kind.as_str()),
                "{kind:?} duplicates an extracted kind"
            );
        }
    }

    #[test]
    fn the_rightmost_phrase_wins() {
        let message = "st0x-hedge: TransferEquityToHedging-0: Job failed after retries: \
                       Raindex vault withdraw failed: inventory vault under-funded for token";

        assert_eq!(
            AlertKind::most_specific_in(message),
            Some(AlertKind::InventoryVaultUnderFunded)
        );
    }

    #[test]
    fn the_first_line_holding_a_phrase_decides() {
        let message = "Portfolio snapshot mark stale\nET day: 2026-10-07\nLow gas";

        assert_eq!(
            AlertKind::most_specific_in(message),
            Some(AlertKind::PortfolioSnapshotMarkStale)
        );
        assert_eq!(
            AlertKind::most_specific_in("no phrase here\nLow gas: wallet"),
            Some(AlertKind::LowGas)
        );
        assert_eq!(AlertKind::most_specific_in("nothing known"), None);
    }

    /// A phrase that contained another would lose every line to it, in the
    /// extractor and here alike, so its rule could never fire. No phrase
    /// contains another, which also rules out ties at one position.
    #[test]
    fn no_kind_contains_another() {
        for kind in EXTRACTED.iter().chain(NOT_EXTRACTED) {
            for other in EXTRACTED.iter().chain(NOT_EXTRACTED) {
                assert!(
                    kind == other || !kind.as_str().contains(other.as_str()),
                    "{kind:?} contains {other:?}"
                );
            }
        }
    }

    fn phrase_or_noise() -> impl Strategy<Value = String> {
        prop_oneof![
            prop::sample::select(rules_file_kinds()),
            "[ a-z:;()0-9-]{0,12}",
            Just("\n".to_string()),
        ]
    }

    proptest! {
        /// The classifier agrees with the extractor regex on any text built
        /// from its phrases, noise and newlines.
        #[test]
        fn classifier_matches_the_extractor_regex(
            parts in prop::collection::vec(phrase_or_noise(), 0..8)
        ) {
            let text = parts.concat();
            let regex_kind = extractor_regex()
                .captures(&text)
                .and_then(|captures| captures.get(1))
                .map(|capture| capture.as_str().to_string());

            prop_assert_eq!(
                AlertKind::most_specific_in(&text).map(|kind| kind.as_str().to_string()),
                regex_kind
            );
        }
    }

    /// The target string is the delivery contract (the downstream metric
    /// filter substring-matches `operational_alert`); the `alert = true`
    /// marker, the `kind` and the message text ride along for structured
    /// queries. All of them must survive refactors verbatim.
    #[tracing_test::traced_test]
    #[tokio::test]
    async fn notify_emits_a_structured_operational_alert_event() {
        LogNotifier
            .notify(AlertKind::LowGas, "Low gas: wallet on base")
            .await
            .unwrap();

        assert!(
            logs_contain("operational_alert"),
            "the alert must be emitted under the operational_alert target"
        );
        assert!(
            logs_contain("alert=true"),
            "the alert marker field must be present for log-based routing"
        );
        assert!(
            logs_contain(r#"kind="Low gas""#),
            "the alert kind must be a structured field"
        );
        assert!(
            logs_contain("Low gas: wallet on base"),
            "the alert text must be the event message"
        );
    }

    /// Every direct `operational_alert` line in the crate carries a `kind`
    /// field, as `Notifier::notify` does.
    #[test]
    fn every_direct_operational_alert_line_carries_a_kind() {
        // Built with concat! so this test's own source does not match it.
        const DIRECT_ALERT_TARGET: &str = concat!("target: \"", "operational_alert", "\"");
        let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut pending = vec![src];
        let mut sites = 0;

        while let Some(path) = pending.pop() {
            if path.is_dir() {
                pending.extend(
                    std::fs::read_dir(&path)
                        .unwrap()
                        .map(|entry| entry.unwrap().path()),
                );
                continue;
            }
            if path.extension().is_none_or(|extension| extension != "rs") {
                continue;
            }
            let source = std::fs::read_to_string(&path).unwrap();
            for (offset, _) in source.match_indices(DIRECT_ALERT_TARGET) {
                let invocation = &source[offset..];
                let invocation = &invocation[..invocation.find(");").unwrap()];
                sites += 1;
                assert!(
                    invocation.contains("kind = AlertKind::")
                        || invocation.contains("kind = kind.as_str()"),
                    "{} has an operational_alert line without a kind: {invocation}",
                    path.display()
                );
            }
        }

        assert!(sites > 20, "found only {sites} direct sites");
    }
}

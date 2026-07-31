//! Policy for aggressive hedging before a multi-day market closure.

use std::time::Duration;

use chrono::{DateTime, Utc};
use metrics::counter;
use tracing::error;

use st0x_execution::{CounterTradeSkipReason, MarketSession, MarketSessionStatus, PostCloseGap};

/// Shared close-flatten decision used by position scanning and hedge pricing.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CloseFlattenPolicy {
    window: chrono::Duration,
}

impl CloseFlattenPolicy {
    pub(crate) fn from_secs(window_secs: u64) -> Result<Self, chrono::OutOfRangeError> {
        chrono::Duration::from_std(Duration::from_secs(window_secs)).map(|window| Self { window })
    }

    #[must_use]
    pub(crate) fn active_window(
        self,
        status: MarketSessionStatus,
        now: DateTime<Utc>,
    ) -> Option<CloseFlattenWindow> {
        if status.session != MarketSession::Extended
            || status.post_close_gap == PostCloseGap::OrdinaryOvernight
        {
            return None;
        }

        let Some(closes_at) = status.extended_session_closes_at else {
            counter!(
                "close_flatten_blocked_total",
                "reason" => "close_time_unknown"
            )
            .increment(1);
            error!(
                post_close_gap = ?status.post_close_gap,
                "extended session close time unknown; skipping close-flattening for this \
                 non-ordinary post-close gap"
            );
            return None;
        };
        let started_at = closes_at - self.window;

        (now >= started_at && now < closes_at).then_some(CloseFlattenWindow { started_at })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CloseFlattenWindow {
    pub(crate) started_at: DateTime<Utc>,
}

/// Shared by `position_check.rs`'s scan-time preflight and `hedge.rs`'s
/// perform-time close-flatten re-preflight, which labels its own rejections
/// identically to the scan-time preflight.
pub(crate) fn preflight_skip_reason_label(reason: &CounterTradeSkipReason) -> &'static str {
    match reason {
        CounterTradeSkipReason::InsufficientEquity { .. } => "insufficient_equity",
        CounterTradeSkipReason::InsufficientBuyingPower { .. } => "insufficient_buying_power",
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;

    use super::*;

    fn status(post_close_gap: PostCloseGap, closes_at: DateTime<Utc>) -> MarketSessionStatus {
        MarketSessionStatus {
            session: MarketSession::Extended,
            extended_session_closes_at: Some(closes_at),
            post_close_gap,
        }
    }

    #[test]
    fn ordinary_overnight_never_activates_close_flattening() {
        let now = Utc::now();
        let policy = CloseFlattenPolicy::from_secs(900).unwrap();

        assert!(
            policy
                .active_window(
                    status(PostCloseGap::OrdinaryOvernight, now + TimeDelta::minutes(5)),
                    now,
                )
                .is_none()
        );
    }

    #[test]
    fn multi_day_and_unknown_gaps_activate_inside_window() {
        let now = Utc::now();
        let closes_at = now + TimeDelta::minutes(5);
        let policy = CloseFlattenPolicy::from_secs(900).unwrap();

        assert!(
            policy
                .active_window(status(PostCloseGap::MultiDayClosure, closes_at), now)
                .is_some()
        );
        assert!(
            policy
                .active_window(status(PostCloseGap::Unknown, closes_at), now)
                .is_some()
        );
    }

    #[test]
    fn long_gap_outside_window_does_not_activate() {
        let now = Utc::now();
        let policy = CloseFlattenPolicy::from_secs(900).unwrap();

        assert!(
            policy
                .active_window(
                    status(PostCloseGap::MultiDayClosure, now + TimeDelta::minutes(16)),
                    now,
                )
                .is_none()
        );
    }

    #[test]
    fn window_activates_exactly_at_started_at() {
        // Half-open window: `now >= started_at`, so the lower bound itself
        // must activate. Pins the left edge so a `>` regression (which would
        // miss the first instant of the window) is caught.
        let now = Utc::now();
        let policy = CloseFlattenPolicy::from_secs(900).unwrap();
        let closes_at = now + TimeDelta::seconds(900);

        let window = policy
            .active_window(status(PostCloseGap::MultiDayClosure, closes_at), now)
            .expect("now == started_at must activate the window");

        assert_eq!(window.started_at, now);
    }

    #[test]
    fn unknown_close_time_does_not_activate_close_flattening() {
        let metrics_handle = crate::metrics::setup().expect("install Prometheus recorder");
        let now = Utc::now();
        let policy = CloseFlattenPolicy::from_secs(900).unwrap();
        let status = MarketSessionStatus {
            session: MarketSession::Extended,
            extended_session_closes_at: None,
            post_close_gap: PostCloseGap::MultiDayClosure,
        };

        assert_eq!(policy.active_window(status, now), None);

        let rendered = metrics_handle.render();
        assert!(rendered.contains("close_flatten_blocked_total{"));
        assert!(rendered.contains("reason=\"close_time_unknown\""));
    }

    #[test]
    fn window_does_not_activate_exactly_at_closes_at() {
        // Half-open window: `now < closes_at`, so the upper bound itself must
        // NOT activate. Pins the right edge so a `<=` regression (which would
        // extend aggressive crossing one tick past session close) is caught.
        let now = Utc::now();
        let policy = CloseFlattenPolicy::from_secs(900).unwrap();
        let closes_at = now;

        assert!(
            policy
                .active_window(status(PostCloseGap::MultiDayClosure, closes_at), now)
                .is_none(),
            "now == closes_at must not activate the window"
        );
    }
}

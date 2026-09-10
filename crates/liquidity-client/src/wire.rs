//! Typed request bodies for the write routes. Each mirrors the bot's request
//! contract in `src/api.rs` field for field, so the client sends exactly the
//! JSON the route deserializes and a contract change is a compile-time edit
//! here rather than a runtime 400.

use serde::Serialize;

/// Body of `POST /transfers/usdc/{id}/reconcile`.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ReconcileUsdcRequest {
    pub(crate) reason: ReconcileUsdcReason,
}

/// The fixed reason vocabulary a USDC reconcile accepts, kebab-cased on the
/// wire exactly as the bot's `ReconcileReasonWire` expects it.
#[derive(Clone, Copy, Serialize, clap::ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum ReconcileUsdcReason {
    FundsMovedManually,
    DepositCreditedOffline,
}

/// Body of `POST /transfers/{kind}/{id}/reconcile` for an equity mint or
/// redemption.
#[derive(Serialize)]
pub(crate) struct ReconcileEquityRequest {
    pub(crate) reason: String,
}

/// Body of `POST /transfers/usdc/{id}/clear-pending-burn`.
#[derive(Serialize)]
pub(crate) struct ClearPendingBurnRequest {
    pub(crate) reason: String,
}

/// Body of `POST /transfers/usdc/{id}/fail`.
#[derive(Serialize)]
pub(crate) struct FailUsdcTransferRequest {
    pub(crate) reason: String,
}

/// Body of `POST /positions/{symbol}/set`.
#[derive(Serialize)]
pub(crate) struct SetPositionRequest {
    pub(crate) target_net: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) price_usdc: Option<String>,
    pub(crate) reason: String,
}

/// Body of `POST /positions/{symbol}/release-hedge`.
#[derive(Serialize)]
pub(crate) struct ReleaseHedgeRequest {
    pub(crate) order_id: String,
    pub(crate) reason: String,
}

/// Body of `POST /portfolio-snapshot/marks`.
#[derive(Serialize)]
pub(crate) struct SetEquityMarkRequest {
    pub(crate) day: String,
    pub(crate) symbol: String,
    pub(crate) usd_mark: String,
    pub(crate) observed_at: String,
    pub(crate) source: String,
    pub(crate) reason: String,
}

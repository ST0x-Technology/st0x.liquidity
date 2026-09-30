//! Typed request bodies for the write routes. Each is a hand-written mirror
//! of the bot's request contract in `src/api.rs`; nothing links the two at
//! compile time, so a contract change on the bot must be reflected here by
//! hand. The request-body tests in `main.rs` pin the JSON each body produces.

use serde::Serialize;

/// Body of `POST /transfers/usdc/{id}/reconcile`.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ReconcileUsdcRequest {
    pub(crate) reason: ReconcileUsdcReason,
    /// The tx that took a signed deposit send's nonce; omitted when absent,
    /// as the bot's `#[serde(default)]` expects.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) superseding_tx: Option<String>,
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

/// Body of `POST /transfers/fail/{kind}/{id}` for an equity mint or
/// redemption.
#[derive(Serialize)]
pub(crate) struct FailEquityTransferRequest {
    pub(crate) reason: String,
}

/// Body of `POST /transfers/usdc/{id}/fail`.
#[derive(Serialize)]
pub(crate) struct FailUsdcTransferRequest {
    pub(crate) reason: String,
}

/// Body of `POST /views/{view}/rebuild`: exactly one of `id` or `all`.
#[derive(Serialize)]
pub(crate) struct RebuildViewRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) id: Option<String>,
    pub(crate) all: bool,
}

/// Body of `POST /cctp/complete-mint`.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CompleteCctpMintRequest {
    pub(crate) burn_tx: String,
    pub(crate) source_chain: &'static str,
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

/// Body of `POST /capital/transfer-usdc`.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TransferUsdcRequest {
    pub(crate) direction: TransferUsdcDirection,
    pub(crate) amount: String,
}

/// The venue a USDC transfer moves funds to, kebab cased on the wire exactly
/// as the bot's transfer request expects it: `to-raindex` is the bot's
/// `AlpacaToBase` and `to-alpaca` its `BaseToAlpaca`.
#[derive(Clone, Copy, Serialize, clap::ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum TransferUsdcDirection {
    /// Transfer to Raindex (onchain venue).
    ToRaindex,
    /// Transfer to Alpaca (offchain venue).
    ToAlpaca,
}

/// Body of `POST /capital/vault-deposit` and `POST /capital/vault-withdraw`.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct VaultTransferRequest {
    pub(crate) chain: &'static str,
    pub(crate) token: String,
    pub(crate) vault_id: String,
    pub(crate) amount: String,
}

/// Body of `POST /capital/vault-withdraw-usdc`.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct VaultWithdrawUsdcRequest {
    pub(crate) chain: &'static str,
    pub(crate) amount: String,
}

/// Body of `POST /capital/cctp-bridge`: exactly one of `amount` or `all`.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CctpBridgeRequest {
    pub(crate) from: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) amount: Option<String>,
    /// Omitted when false, unlike `RebuildViewRequest::all`: the bot's body
    /// carries either `amount` or `all: true`, never both keys.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub(crate) all: bool,
}

/// Body of `POST /capital/reset-allowance`.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ResetAllowanceRequest {
    pub(crate) chain: &'static str,
}

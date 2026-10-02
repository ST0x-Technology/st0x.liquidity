//! Typed request bodies for the write routes. Each is a hand-written mirror
//! of the bot's request contract in `src/api.rs`; nothing links the two at
//! compile time, so a contract change on the bot must be reflected here by
//! hand. The request-body tests in `main.rs` pin the JSON each body produces.

use std::str::FromStr;

use serde::Serialize;
use uuid::Uuid;

/// A positive decimal amount as the user typed it: ASCII digits with at most
/// one `.` that has digits on both sides, and at least one nonzero digit.
/// The bot re-validates precision; this only refuses input that can never be
/// valid, before authenticating or calling the bot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub(crate) struct DecimalAmount(String);

impl FromStr for DecimalAmount {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let (whole, fraction) = raw.split_once('.').unwrap_or((raw, "0"));
        let all_digits =
            |part: &str| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit());
        if !all_digits(whole) || !all_digits(fraction) {
            return Err(format!(
                "`{raw}` is not a decimal amount (digits with an optional `.` \
                 between digits, for example 100 or 1.5)"
            ));
        }
        if raw.bytes().all(|byte| byte == b'0' || byte == b'.') {
            return Err(format!("`{raw}` is zero; the amount must be positive"));
        }
        Ok(Self(raw.to_owned()))
    }
}

/// Parses `raw` as `0x` followed by exactly `digits` hex digits, keeping the
/// spelling as typed. Only the lowercase prefix, which the bot's hex decoding
/// accepts.
fn parse_prefixed_hex(raw: &str, digits: usize, what: &str) -> Result<String, String> {
    let hex = raw
        .strip_prefix("0x")
        .ok_or_else(|| format!("`{raw}` is not {what}: missing the 0x prefix"))?;
    if hex.len() != digits || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(format!(
            "`{raw}` is not {what}: expected 0x followed by exactly {digits} hex digits"
        ));
    }
    Ok(raw.to_owned())
}

/// An EVM address as the user typed it: `0x` plus exactly 40 hex digits.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub(crate) struct EvmAddress(String);

impl FromStr for EvmAddress {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        parse_prefixed_hex(raw, 40, "an EVM address").map(Self)
    }
}

/// A Raindex vault id as the user typed it: `0x` plus exactly 64 hex digits.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub(crate) struct VaultId(String);

impl FromStr for VaultId {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        parse_prefixed_hex(raw, 64, "a vault id").map(Self)
    }
}

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
#[serde(rename_all = "camelCase")]
pub(crate) struct ReconcileEquityRequest {
    pub(crate) reason: String,
    /// The tx that took a redemption's signed vault withdrawal nonce;
    /// omitted when absent, as the bot's `#[serde(default)]` expects.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) superseding_tx: Option<String>,
}

/// Body of `POST /transfers/equity_redemption/{id}/adopt-withdrawal`.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AdoptWithdrawalRequest {
    pub(crate) reason: String,
    pub(crate) replacement_tx: String,
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
    pub(crate) amount: DecimalAmount,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) chain: Option<&'static str>,
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
    pub(crate) token: EvmAddress,
    pub(crate) vault_id: VaultId,
    pub(crate) amount: DecimalAmount,
}

/// Body of `POST /capital/vault-withdraw-usdc`.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct VaultWithdrawUsdcRequest {
    pub(crate) chain: &'static str,
    pub(crate) amount: DecimalAmount,
}

/// Body of `POST /capital/cctp-bridge`: the run's operation id and exactly
/// one of `amount` or `all`.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CctpBridgeRequest {
    pub(crate) operation_id: Uuid,
    pub(crate) from: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) amount: Option<DecimalAmount>,
    /// Omitted when false, unlike `RebuildViewRequest::all`: the bot's body
    /// carries either `amount` or `all: true`, never both keys.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub(crate) all: bool,
}

/// Body of `POST /capital/cctp-burn-supersede`.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CctpBurnSupersedeRequest {
    pub(crate) operation_id: Uuid,
    pub(crate) superseding_tx: String,
}

/// Body of `POST /capital/reset-allowance`.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ResetAllowanceRequest {
    pub(crate) chain: &'static str,
}

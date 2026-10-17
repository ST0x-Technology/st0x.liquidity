//! The capital tier of the ops API (`/liquidity-capital/*`): typed routes for
//! the capital verbs of `st0x-cli`, run inside the bot so they sign with its
//! own wallets and share their nonce state instead of building a second
//! signer.
//!
//! No route waits on CCTP attestation, on USDC or equity settlement, or on the
//! confirmations of the transaction it answers with, since the ops load
//! balancer times out first: `transfer-usdc` and `transfer-equity` enqueue the
//! transfer on the bot's own transfer workers and return its id, and every
//! route that sends a transaction answers at its broadcast through
//! `answer_from_detached` and confirms it on the task. Only a prerequisite
//! approve, when an allowance is short, is awaited before that broadcast. The
//! routes that send transactions, and the `transfer-usdc` and
//! `transfer-equity` enqueues, run through `spawn_detached`, so a dropped
//! request cannot cancel them midway (between broadcast and receipt, or
//! between the enqueue and the guard claim it keeps), and graceful shutdown
//! waits for them. Every capital route refuses until startup completes, like
//! `process-tx`.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use alloy::primitives::{Address, B256, Bytes, TxHash, U256};
use alloy::providers::{Provider as _, RootProvider};
use alloy::sol_types::SolCall as _;
use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use futures_util::FutureExt as _;
use futures_util::future::BoxFuture;
use rain_math_float::Float;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use tokio::sync::{Mutex, OwnedMutexGuard};
use tracing::{error, info, warn};

use st0x_bridge::Bridge;
use st0x_bridge::corridor::UsdcCorridor;
use st0x_config::{BrokerCtx, Ctx, HedgedChain, OnchainWalletCtx};
use st0x_event_sorcery::Store;
use st0x_evm::{Chain, Evm, IERC20, MinedTx, OpenChainErrorRegistry, PreparedTransaction, Wallet};
use st0x_execution::{AlpacaWalletService, TransferStatus};
use st0x_finance::{FractionalShares, HasZero, Positive, Symbol, Usdc};
use st0x_float_serde::format_float_with_fallback;
use st0x_raindex::{Raindex, RaindexError, RaindexService, RaindexVaultId, RevokeOutcome};
use st0x_tokenization::{
    AlpacaTokenizationService, IssuerRequestId, RedemptionSendReceipt, TokenizationRequestStatus,
    Tokenizer, TokenizerError,
};
use st0x_wrapper::{Wrapper, WrapperError, WrapperService};

use super::{
    ErrorResponse, OpsError, ResumeLock, UsdcDriverPauseRequest, answer_from_detached,
    cctp_source_chain_wire_name, manual_equity_transfer_error_response, ops_command_error,
    ops_precondition_error, quiesce_usdc_driver, spawn_detached, usdc_resume_error_response,
};
use crate::AppState;
use crate::alerts::AlertKind;
use crate::alpaca_deposit::{
    AlpacaDepositOperation, AlpacaDepositOperationCommand, AlpacaDepositOperationId,
    AlpacaDepositOutcome, AlpacaDepositStatus, DEPOSIT_TRANSFER_NOTE, DepositTransfer,
    DepositWallet, RestoredAlpacaDeposits, deposit_status, restore_pending_alpaca_deposits,
    settle_pending_deposit,
};
use crate::cctp_burn::{
    BotCctpBridge, BurnNotSuperseded, BurnReceiptFate, CctpBurnOperation, CctpBurnOperationCommand,
    CctpBurnOperationId, CctpBurnStatus, CctpSourceChain, NonceTakenBy, RequestedBurn,
    bot_cctp_bridge, burn_receipt_fate, record_burn_fate, scrubbed, settle_burn_from_receipt,
    signed_by_source_wallet, verify_burn_superseded,
};
use crate::native_gas::{GasReadiness, TransferGasRoute};
use crate::operations_audit::{Audited, AuditedRequest};
use crate::rebalancing::trigger::InProgressGuard;
use crate::rebalancing::trigger::allocation::{
    ALPACA_QUANTITY_MAX_DECIMAL_PLACES, PlannedDirection,
};
use crate::rebalancing::usdc::{
    UsdcDriverPause, alpaca_usdc_deposit_address, deposit_send_required_confirmations,
};
use crate::rebalancing::{RebalancingService, UsdcResumeError, to_wrapped_equities};
use crate::tokenized_equity_mint::{TOKENIZED_EQUITY_DECIMALS, TokenizedEquityMint};
use crate::usdc_rebalance::RebalanceDirection;

/// The direction of a manual USDC or equity transfer, spelled on the wire
/// like the `st0x-cli` `--direction` values (`to-raindex`, `to-alpaca`).
#[derive(Deserialize, Serialize, Clone, Copy)]
#[serde(rename_all = "kebab-case")]
enum TransferDirectionWire {
    ToRaindex,
    ToAlpaca,
}

impl TransferDirectionWire {
    /// The wire spelling, which the audit record names the direction by.
    const fn wire_name(self) -> &'static str {
        match self {
            Self::ToRaindex => "to-raindex",
            Self::ToAlpaca => "to-alpaca",
        }
    }
}

impl From<TransferDirectionWire> for RebalanceDirection {
    fn from(wire: TransferDirectionWire) -> Self {
        match wire {
            TransferDirectionWire::ToRaindex => Self::AlpacaToBase,
            TransferDirectionWire::ToAlpaca => Self::BaseToAlpaca,
        }
    }
}

impl From<TransferDirectionWire> for PlannedDirection {
    fn from(wire: TransferDirectionWire) -> Self {
        match wire {
            TransferDirectionWire::ToRaindex => Self::Mint,
            TransferDirectionWire::ToAlpaca => Self::Redemption,
        }
    }
}

/// Wire contract for the manual USDC transfer route.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct TransferUsdcRequest {
    direction: TransferDirectionWire,
    /// Decimal USDC amount; must be positive.
    amount: String,
    /// Chain of the served cash corridor to run on, as for
    /// `st0x-cli transfer-usdc --chain`; may be left out while the build
    /// serves one corridor.
    #[serde(default)]
    chain: Option<Chain>,
}

impl AuditedRequest for TransferUsdcRequest {
    fn audit_targets(&self) -> Vec<String> {
        std::iter::once(self.direction.wire_name().to_string())
            .chain(self.chain.iter().map(ToString::to_string))
            .collect()
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct TransferUsdcResponse {
    transfer_id: String,
    direction: TransferDirectionWire,
    amount: String,
    outcome: &'static str,
}

/// Wire contract for the manual equity transfer route.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct TransferEquityRequest {
    direction: TransferDirectionWire,
    /// Ticker of the tokenized equity, as for `st0x-cli transfer-equity
    /// --symbol`.
    symbol: String,
    /// Decimal shares; must be positive, with at most the nine decimals the
    /// tokenization API takes.
    quantity: String,
    /// Chain whose Raindex vault the shares land in (`to-raindex`) or leave
    /// (`to-alpaca`), as for `st0x-cli transfer-equity --network`.
    chain: Chain,
    /// Issuer request id of an existing mint to resume instead of starting
    /// one, as for `st0x-cli transfer-equity --issuer-request-id`; only with
    /// `to-raindex`.
    #[serde(default)]
    issuer_request_id: Option<IssuerRequestId>,
}

impl AuditedRequest for TransferEquityRequest {
    fn audit_targets(&self) -> Vec<String> {
        [
            self.direction.wire_name().to_string(),
            self.symbol.clone(),
            self.chain.to_string(),
        ]
        .into_iter()
        .chain(self.issuer_request_id.iter().map(ToString::to_string))
        .collect()
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct TransferEquityResponse {
    transfer_id: String,
    direction: TransferDirectionWire,
    symbol: String,
    quantity: String,
    chain: Chain,
    outcome: &'static str,
}

/// Wire contract shared by the vault deposit and vault withdraw routes.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct VaultRequest {
    chain: Chain,
    token: Address,
    vault_id: B256,
    /// Decimal token amount; must be positive.
    amount: String,
}

impl AuditedRequest for VaultRequest {
    fn audit_targets(&self) -> Vec<String> {
        vec![
            self.chain.to_string(),
            self.token.to_string(),
            self.vault_id.to_string(),
        ]
    }
}

/// Wire contract for the vault withdraw USDC route: the chain's settlement
/// stable out of its first configured cash vault.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct VaultWithdrawUsdcRequest {
    chain: Chain,
    /// Decimal amount of the settlement stable; must be positive.
    amount: String,
}

impl AuditedRequest for VaultWithdrawUsdcRequest {
    fn audit_targets(&self) -> Vec<String> {
        vec![self.chain.to_string()]
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct VaultDepositResponse {
    chain: Chain,
    token: Address,
    vault_id: B256,
    amount: String,
    /// The amount in the token's smallest unit.
    amount_raw: String,
    decimals: u8,
    deposit_tx: TxHash,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct VaultWithdrawResponse {
    chain: Chain,
    token: Address,
    vault_id: B256,
    amount: String,
    /// The amount in the token's smallest unit.
    amount_raw: String,
    decimals: u8,
    withdraw_tx: TxHash,
}

/// Wire contract for the CCTP burn route: the client's operation id, and
/// exactly one of `amount` and `all: true`. A request whose operation id
/// already holds a burn reports that burn instead of burning again.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct CctpBridgeRequest {
    operation_id: CctpBurnOperationId,
    from: CctpSourceChain,
    #[serde(default)]
    amount: Option<String>,
    #[serde(default)]
    all: bool,
}

impl AuditedRequest for CctpBridgeRequest {
    fn audit_targets(&self) -> Vec<String> {
        vec![
            self.operation_id.to_string(),
            cctp_source_chain_wire_name(self.from).to_string(),
        ]
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct CctpBridgeResponse {
    operation_id: CctpBurnOperationId,
    burn_tx: TxHash,
    source_chain: Chain,
    destination_chain: Chain,
    /// The burned amount in USDC base units (6 decimals).
    amount_raw: String,
    status: CctpBurnStatus,
}

impl CctpBridgeResponse {
    fn of(
        operation_id: CctpBurnOperationId,
        operation: &CctpBurnOperation,
        status: CctpBurnStatus,
    ) -> Self {
        Self {
            operation_id,
            burn_tx: operation.effective_burn_tx(),
            source_chain: operation.source.chain(),
            destination_chain: operation.source.destination_chain(),
            amount_raw: operation.amount.to_string(),
            status,
        }
    }
}

/// Wire contract for settling a pending CCTP burn that can never mine: the
/// operation that holds it, and the mined tx that took its nonce.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct CctpBurnSupersedeRequest {
    operation_id: CctpBurnOperationId,
    superseding_tx: TxHash,
}

impl AuditedRequest for CctpBurnSupersedeRequest {
    fn audit_targets(&self) -> Vec<String> {
        vec![
            self.operation_id.to_string(),
            self.superseding_tx.to_string(),
        ]
    }
}

/// Wire contract for the orderbook allowance reset route.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ResetAllowanceRequest {
    chain: Chain,
}

impl AuditedRequest for ResetAllowanceRequest {
    fn audit_targets(&self) -> Vec<String> {
        vec![self.chain.to_string()]
    }
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum ResetAllowanceOutcome {
    Revoked,
    AlreadyZero,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ResetAllowanceResponse {
    chain: Chain,
    token: Address,
    spender: Address,
    outcome: ResetAllowanceOutcome,
    /// The revoke transaction; `null` when the allowance was already zero.
    tx: Option<TxHash>,
}

/// Wire contract shared by the wrap and unwrap routes.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct WrapRequest {
    /// Ticker of the tokenized equity, as for `st0x-cli wrap-equity
    /// --symbol`.
    symbol: String,
    /// Decimal shares with at most 18 decimals: underlying shares to wrap, or
    /// wrapped shares to unwrap, as for `--quantity`.
    quantity: String,
    /// Chain whose trading table lists the equity and its wrapper, as for
    /// `--network`.
    chain: Chain,
}

impl AuditedRequest for WrapRequest {
    fn audit_targets(&self) -> Vec<String> {
        vec![self.symbol.clone(), self.chain.to_string()]
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct WrapResponse {
    chain: Chain,
    symbol: String,
    quantity: String,
    wrap_tx: TxHash,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct UnwrapResponse {
    chain: Chain,
    symbol: String,
    quantity: String,
    unwrap_tx: TxHash,
}

/// Wire contract for the drain equity route.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct DrainEquityRequest {
    /// Ticker of the tokenized equity to drain.
    symbol: String,
    /// Decimal wrapped shares to take out of the vault, with at most 18
    /// decimals.
    quantity: String,
    /// Chain whose listing, vault and wallet the drain runs on.
    chain: Chain,
    /// The Raindex vault holding the wrapped shares, as for
    /// `vault-withdraw --vault-id`.
    vault_id: B256,
}

impl AuditedRequest for DrainEquityRequest {
    fn audit_targets(&self) -> Vec<String> {
        vec![
            self.symbol.clone(),
            self.chain.to_string(),
            self.vault_id.to_string(),
        ]
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct DrainEquityResponse {
    chain: Chain,
    symbol: String,
    quantity: String,
    vault_id: B256,
    redemption_wallet: Address,
    /// The vault withdrawal, the drain's first tx; the unwrap and the
    /// redemption transfer follow on the bot and appear in its logs.
    withdraw_tx: TxHash,
}

/// Wire contract for the redeem route.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct RedeemRequest {
    /// Ticker of the tokenized equity, as for `st0x-cli alpaca-redeem
    /// --symbol`.
    symbol: String,
    /// Decimal underlying shares with at most 18 decimals.
    quantity: String,
    /// Chain whose wallet sends the shares, as for `--network`.
    chain: Chain,
}

impl AuditedRequest for RedeemRequest {
    fn audit_targets(&self) -> Vec<String> {
        vec![self.symbol.clone(), self.chain.to_string()]
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct RedeemResponse {
    chain: Chain,
    symbol: String,
    quantity: String,
    redemption_wallet: Address,
    /// The underlying the chain's wrapper attested as its `asset()`.
    token: Address,
    transfer_tx: TxHash,
}

/// Wire contract for the Alpaca deposit route: the client's operation id and
/// the amount. It names no chain and no destination: the send is always
/// Ethereum USDC from the bot's Ethereum wallet to the deposit address Alpaca
/// gives for the bot's own account. A request whose operation id already
/// holds a transfer reports that transfer instead of sending again.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct AlpacaDepositRequest {
    operation_id: AlpacaDepositOperationId,
    /// Decimal USDC with at most 6 decimals, as for `st0x-cli
    /// alpaca-deposit --amount`.
    amount: String,
}

impl AuditedRequest for AlpacaDepositRequest {
    fn audit_targets(&self) -> Vec<String> {
        vec![self.operation_id.to_string(), Chain::Ethereum.to_string()]
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct AlpacaDepositResponse {
    operation_id: AlpacaDepositOperationId,
    chain: Chain,
    /// Alpaca's USDC deposit address on Ethereum for the bot's account, the
    /// one the recorded transfer pays.
    deposit_address: Address,
    amount: String,
    /// The amount in USDC base units (6 decimals).
    amount_raw: String,
    transfer_tx: TxHash,
    status: AlpacaDepositStatus,
}

impl AlpacaDepositResponse {
    fn of(
        request: &DepositRequest,
        transfer: &DepositTransfer,
        status: AlpacaDepositStatus,
    ) -> Self {
        Self {
            operation_id: request.operation_id,
            chain: Chain::Ethereum,
            deposit_address: transfer.deposit_address,
            amount: request.amount.clone(),
            amount_raw: transfer.amount.to_string(),
            transfer_tx: transfer.transfer_tx(),
            status,
        }
    }
}

/// Starts a USDC transfer between Alpaca and Raindex on the bot's own
/// transfer worker and returns the new transfer's id without waiting for it.
/// Progress is observed like any rebalance. Mirrors `st0x-cli transfer-usdc`,
/// except that the bot drives the whole transfer.
///
/// Input validation and the startup gate come first, as on every capital
/// route. The rest is ordered like `resume_usdc_transfer`: the resume lock,
/// the recovery handle, the driver pause, then the enqueue gates. Everything
/// after the startup gate runs detached: the enqueue commits before the
/// start keeps its corridor claim, so a request dropped between the two would
/// release the claim and the driver pause with the job still queued, letting
/// the trigger start a second transfer on the corridor.
pub(super) async fn transfer_usdc(
    State(state): State<AppState>,
    Audited(request): Audited<TransferUsdcRequest>,
) -> Result<Json<TransferUsdcResponse>, (StatusCode, Json<ErrorResponse>)> {
    let wire_direction = request.direction;
    let direction = RebalanceDirection::from(wire_direction);
    // The worker converts the amount to USDC base units before it records
    // anything, so an amount off the six decimal grid is refused here.
    let (amount, _) = positive_usdc(&request.amount)?;
    require_startup_complete(&state, "transfer-usdc")?;

    let detached_tasks = state.detached_tasks.clone();
    let transfer_id = spawn_detached(&detached_tasks, "transfer-usdc", amount, async move {
        let _guard = state.resume_lock.0.try_lock().map_err(|_| {
            (
                StatusCode::CONFLICT,
                Json(ErrorResponse {
                    error: "A resume or recheck operation is already in progress".to_string(),
                }),
            )
        })?;

        let handle = state.recovery.get().ok_or_else(recovery_not_ready)?;

        // Quiesce the workers so the start's preflight (job row dedupe and
        // durable holder scan) and its enqueue cannot straddle an execution in
        // flight.
        let _driver_paused = quiesce_usdc_driver(
            &handle.usdc_driver_pause,
            UsdcDriverPauseRequest::ManualTransfer { direction },
        )
        .await?;

        let transfer_id = handle
            .rebalancing_service
            .start_manual_usdc_transfer(&state.pool, direction, amount, request.chain)
            .await
            .map_err(|error| {
                error!(?error, ?direction, %amount, "Failed to start manual USDC transfer");
                let (status, message) = usdc_resume_error_response(&error);
                (status, Json(ErrorResponse { error: message }))
            })?;

        info!(%transfer_id, ?direction, %amount, "Manual USDC transfer enqueued via API");
        Ok::<_, OpsError>(transfer_id)
    })?
    .await??;

    Ok(Json(TransferUsdcResponse {
        transfer_id: transfer_id.to_string(),
        direction: wire_direction,
        amount: amount.inner().to_string(),
        outcome: "enqueued",
    }))
}

/// Starts a tokenized equity transfer between Alpaca and Raindex on the
/// bot's own equity transfer workers, the path the equity rebalancer uses,
/// and returns its id without waiting for it: a mint of `quantity` shares of
/// `symbol` into the `chain` vault (`to-raindex`), or a redemption out of it
/// back to Alpaca (`to-alpaca`). The id is the mint's issuer request id or
/// the redemption aggregate id, which `debug recheck` and
/// `read transfer-events` take. Mirrors `st0x-cli transfer-equity`, except
/// that the bot drives the whole transfer.
///
/// Ordered like `transfer_usdc`: input validation and the startup gate, then
/// the resume lock and the recovery handle, then the enqueue gates of
/// `start_manual_equity_transfer`. Unlike `transfer_usdc` no driver is
/// paused: the equity workers have no pause control, and the symbol's
/// transfer guard, its job row dedupe and its Position reservation already
/// keep a start from racing a transfer in flight. Everything after the
/// startup gate runs detached, so a request dropped between the enqueue and
/// the guard it keeps cannot release the guard and the reservation with the
/// job still queued. The detached task holds a projection write slot for the
/// whole start, since it writes Position events, so a view rebuild cannot
/// replay them while the live projection also applies them.
///
/// With `issuerRequestId` it resumes that existing mint instead and never
/// starts one; see [`resume_equity_mint`].
pub(super) async fn transfer_equity(
    State(state): State<AppState>,
    Audited(request): Audited<TransferEquityRequest>,
) -> Result<Json<TransferEquityResponse>, (StatusCode, Json<ErrorResponse>)> {
    let TransferEquityRequest {
        direction: wire_direction,
        symbol,
        quantity,
        chain,
        issuer_request_id,
    } = request;
    let direction = PlannedDirection::from(wire_direction);
    let symbol = Symbol::new(symbol.as_str())
        .map_err(|error| ops_precondition_error(format!("invalid symbol {symbol:?}: {error}")))?;
    let quantity = positive_shares(&quantity)?;
    if let Some(issuer_request_id) = issuer_request_id {
        let TransferDirectionWire::ToRaindex = wire_direction else {
            return Err(ops_precondition_error(
                "issuerRequestId names a mint to resume, so it applies only to direction \
                 to-raindex",
            ));
        };
        require_startup_complete(&state, "transfer-equity")?;
        return resume_equity_mint(state, issuer_request_id, symbol, quantity, chain)
            .await
            .map(Json);
    }
    require_startup_complete(&state, "transfer-equity")?;

    let detached_tasks = state.detached_tasks.clone();
    let task_symbol = symbol.clone();
    let transfer_id = spawn_detached(
        &detached_tasks,
        "transfer-equity",
        symbol.clone(),
        async move {
            // Admitted before the recovery lock, like the other recovery
            // routes: a start parked behind a rebuild holds no lock.
            let _projection_write = state.projection_maintenance.enter().await;

            let _guard = state.resume_lock.0.try_lock().map_err(|_| {
                (
                    StatusCode::CONFLICT,
                    Json(ErrorResponse {
                        error: "A resume or recheck operation is already in progress".to_string(),
                    }),
                )
            })?;

            let handle = state.recovery.get().ok_or_else(recovery_not_ready)?;

            let transfer_id = handle
                .rebalancing_service
                .start_manual_equity_transfer(&task_symbol, direction, quantity, chain)
                .await
                .map_err(|error| {
                    error!(
                        ?error,
                        symbol = %task_symbol,
                        ?direction,
                        %quantity,
                        %chain,
                        "Failed to start manual equity transfer"
                    );
                    let (status, message) = manual_equity_transfer_error_response(&error);
                    (status, Json(ErrorResponse { error: message }))
                })?;

            info!(
                %transfer_id,
                symbol = %task_symbol,
                ?direction,
                %quantity,
                %chain,
                "Manual equity transfer enqueued via API"
            );
            Ok::<_, OpsError>(transfer_id)
        },
    )?
    .await??;

    Ok(Json(TransferEquityResponse {
        transfer_id: transfer_id.to_string(),
        direction: wire_direction,
        symbol: symbol.to_string(),
        quantity: quantity.inner().to_string(),
        chain,
        outcome: "enqueued",
    }))
}

/// Resumes the mint `id` (one `st0x-cli transfer-equity` started, or the
/// bot) and never starts a new one. It drives the mint through
/// `CrossVenueEquityTransfer::resume_mint`, the path `debug resume` and
/// `debug recheck mint` take for an interrupted mint: it continues from the
/// aggregate's persisted state, and a mint still at `MintRequested` replays
/// its request under its own issuer request id, which Alpaca dedupes, so it
/// cannot mint twice. A mint the CLI started has no job row; startup already
/// rebuilt its tracking and guard and restored its Position reservation, which
/// the trigger releases when the mint ends.
///
/// Ordered like a fresh start: the projection write slot, the resume lock and
/// the recovery handle, then the checks: the mint must exist (404), its
/// recorded chain and symbol must be the request's (422 naming the recorded
/// ones, since another chain's orderbook and issuer wallet or another
/// symbol's Position and vault would be used), its recorded quantity must be
/// the request's (422 naming it, since a different one means the operator has
/// the wrong id), it must not have ended (409),
/// and nothing else may drive it (409, [`RebalancingService::mint_driver`]).
/// The route then answers `resumed` with the recorded quantity and runs the
/// resume on the detached task, holding the slot and the lock until it ends,
/// as `debug resume` holds them: the resume polls Alpaca and confirms the
/// wrap and the deposit, which outlast the load balancer's 60 second cut.
async fn resume_equity_mint(
    state: AppState,
    id: IssuerRequestId,
    symbol: Symbol,
    quantity: Positive<FractionalShares>,
    chain: Chain,
) -> Result<TransferEquityResponse, OpsError> {
    let detached_tasks = state.detached_tasks.clone();
    answer_from_detached(&detached_tasks, "transfer-equity", id.clone(), async move {
        // Admitted before the recovery lock, like the other recovery routes:
        // a resume parked behind a rebuild holds no lock.
        let projection_write = state.projection_maintenance.enter().await;
        let resume_guard = Arc::clone(&state.resume_lock.0)
            .try_lock_owned()
            .map_err(|_| {
                conflict("A resume or recheck operation is already in progress".to_string())
            })?;
        let handle = state.recovery.get().ok_or_else(recovery_not_ready)?;

        let recorded = st0x_event_sorcery::load_entity::<TokenizedEquityMint>(&state.pool, &id)
            .await
            .map_err(|error| {
                error!(%id, ?error, "Failed to load the mint to resume");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: "Failed to load the mint to resume".to_string(),
                    }),
                )
            })?
            .ok_or_else(|| {
                (
                    StatusCode::NOT_FOUND,
                    Json(ErrorResponse {
                        error: format!(
                            "No mint with issuer request id {id} exists; a resume never starts \
                             a new one"
                        ),
                    }),
                )
            })?;
        let recorded_chain = recorded.chain();
        if recorded_chain != chain {
            return Err(unprocessable(format!(
                "mint {id} was requested on {recorded_chain}; resuming it on {chain} would use \
                 another chain's orderbook and issuer wallet. Resume it with chain \
                 {recorded_chain}"
            )));
        }
        let recorded_symbol = recorded.symbol();
        if *recorded_symbol != symbol {
            return Err(unprocessable(format!(
                "mint {id} was requested for {recorded_symbol}; resuming it as {symbol} would \
                 use another symbol's Position and vault. Resume it with symbol \
                 {recorded_symbol}"
            )));
        }
        let recorded_quantity = FractionalShares::new(recorded.quantity());
        if recorded_quantity != quantity.inner() {
            return Err(unprocessable(format!(
                "mint {id} was requested for {recorded_quantity} shares, not {}; check the id, \
                 or resume it with quantity {recorded_quantity}",
                quantity.inner()
            )));
        }
        if let Some(ended) = ended_mint(&recorded) {
            return Err(conflict(format!(
                "mint {id} has already {ended}; there is nothing to resume"
            )));
        }
        let driver = handle
            .rebalancing_service
            .mint_driver(&id, &symbol)
            .await
            .map_err(|error| {
                error!(%id, ?error, "Failed to check what drives the mint to resume");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: "Failed to check whether the bot already drives the mint"
                            .to_string(),
                    }),
                )
            })?;
        if let Some(driver) = driver {
            warn!(%id, %symbol, %driver, "Mint resume refused: the mint already has a driver");
            return Err(conflict(format!(
                "mint {id} is already driven by {driver}; retry once it has stopped"
            )));
        }

        let quantity = recorded_quantity;
        info!(%id, %symbol, %chain, %quantity, "Equity mint resume started via API");
        let response = TransferEquityResponse {
            transfer_id: id.to_string(),
            direction: TransferDirectionWire::ToRaindex,
            symbol: symbol.to_string(),
            quantity: quantity.to_string(),
            chain,
            outcome: "resumed",
        };
        let transfer = Arc::clone(&handle.transfer);
        let resume = async move {
            let _held = (projection_write, resume_guard);
            match transfer.resume_mint(&id).await {
                Ok(()) => info!(%id, %symbol, %chain, "Equity mint resumed via API"),
                Err(error) => error!(
                    %id, %symbol, %chain, ?error,
                    "Equity mint resume via API failed; check the mint's events before \
                     resuming it again"
                ),
            }
        };
        Ok::<_, OpsError>((response, resume))
    })
    .await
}

/// How a mint that can no longer progress ended, or `None` while it can.
const fn ended_mint(mint: &TokenizedEquityMint) -> Option<&'static str> {
    match mint {
        TokenizedEquityMint::DepositedIntoRaindex { .. } => Some("been deposited into Raindex"),
        TokenizedEquityMint::Failed { .. } => Some("failed"),
        TokenizedEquityMint::Reconciled { .. } => Some("been reconciled"),
        TokenizedEquityMint::MintRequested { .. }
        | TokenizedEquityMint::MintAccepted { .. }
        | TokenizedEquityMint::TokensReceived { .. }
        | TokenizedEquityMint::WrapSubmitted { .. }
        | TokenizedEquityMint::TokensWrapped { .. }
        | TokenizedEquityMint::VaultDepositSubmitted { .. } => None,
    }
}

/// Deposits `amount` of `token` into a Raindex vault on the chosen chain:
/// approve then deposit, with the token's decimals read onchain. Mirrors
/// `st0x-cli vault-deposit`.
pub(super) async fn vault_deposit(
    State(state): State<AppState>,
    Audited(request): Audited<VaultRequest>,
) -> Result<Json<VaultDepositResponse>, (StatusCode, Json<ErrorResponse>)> {
    let VaultRequest {
        chain,
        token,
        vault_id,
        amount,
    } = request;
    let amount = positive_amount(&amount, std::convert::identity)?;
    let (trading, wallet) = hedged_chain_signer(&state, chain)?;
    require_startup_complete(&state, VaultOperation::Deposit.route())?;

    let VaultOutcome {
        decimals,
        amount_raw,
        tx,
    } = run_vault_operation(
        &state,
        VaultOperation::Deposit,
        trading,
        wallet,
        VaultTarget {
            chain,
            token,
            vault_id,
            amount,
        },
    )
    .await?;

    Ok(Json(VaultDepositResponse {
        chain,
        token,
        vault_id,
        amount: format_float_with_fallback(&amount.inner()),
        amount_raw: amount_raw.to_string(),
        decimals,
        deposit_tx: tx,
    }))
}

/// Withdraws `amount` of `token` from a Raindex vault on the chosen chain to
/// the bot's wallet there. Mirrors `st0x-cli vault-withdraw`.
pub(super) async fn vault_withdraw(
    State(state): State<AppState>,
    Audited(request): Audited<VaultRequest>,
) -> Result<Json<VaultWithdrawResponse>, (StatusCode, Json<ErrorResponse>)> {
    let VaultRequest {
        chain,
        token,
        vault_id,
        amount,
    } = request;
    let amount = positive_amount(&amount, std::convert::identity)?;
    let (trading, wallet) = hedged_chain_signer(&state, chain)?;
    require_startup_complete(&state, VaultOperation::Withdraw.route())?;

    let VaultOutcome {
        decimals,
        amount_raw,
        tx,
    } = run_vault_operation(
        &state,
        VaultOperation::Withdraw,
        trading,
        wallet,
        VaultTarget {
            chain,
            token,
            vault_id,
            amount,
        },
    )
    .await?;

    Ok(Json(VaultWithdrawResponse {
        chain,
        token,
        vault_id,
        amount: format_float_with_fallback(&amount.inner()),
        amount_raw: amount_raw.to_string(),
        decimals,
        withdraw_tx: tx,
    }))
}

/// Withdraws the chosen chain's settlement stable from its first configured
/// cash vault. Mirrors `st0x-cli vault-withdraw-usdc`, which resolves the
/// token and vault the same way and then runs the plain vault withdraw.
pub(super) async fn vault_withdraw_usdc(
    State(state): State<AppState>,
    Audited(request): Audited<VaultWithdrawUsdcRequest>,
) -> Result<Json<VaultWithdrawResponse>, (StatusCode, Json<ErrorResponse>)> {
    let VaultWithdrawUsdcRequest { chain, amount } = request;
    let amount = positive_amount(&amount, std::convert::identity)?;
    // The wallet is required before the cash vault lookup, as in the CLI.
    let (trading, wallet) = hedged_chain_signer(&state, chain)?;
    let token = chain.settlement_stable().address;

    let vault_ids = trading
        .assets
        .cash
        .as_ref()
        .map_or(&[][..], |cash| cash.vault_ids.as_slice());
    let Some(&vault_id) = vault_ids.first() else {
        return Err(ops_precondition_error(format!(
            "vault_ids in [chains.{chain}.trading.assets.cash] is required but not configured"
        )));
    };
    if vault_ids.len() > 1 {
        warn!(
            %chain,
            configured = vault_ids.len(),
            %vault_id,
            "Several cash vaults configured; withdrawing from the first one"
        );
    }
    require_startup_complete(&state, VaultOperation::WithdrawUsdc.route())?;

    let VaultOutcome {
        decimals,
        amount_raw,
        tx,
    } = run_vault_operation(
        &state,
        VaultOperation::WithdrawUsdc,
        trading,
        wallet,
        VaultTarget {
            chain,
            token,
            vault_id,
            amount,
        },
    )
    .await?;

    Ok(Json(VaultWithdrawResponse {
        chain,
        token,
        vault_id,
        amount: format_float_with_fallback(&amount.inner()),
        amount_raw: amount_raw.to_string(),
        decimals,
        withdraw_tx: tx,
    }))
}

/// Burns USDC on the source chain through CCTP toward the bot's wallet on the
/// other chain and returns the burn tx as soon as the burn is broadcast. It
/// never waits for the attestation or mints; the operator finishes with the
/// existing `cctp complete-mint` route, passing the burn tx and this source
/// chain. Mirrors the burn step of `st0x-cli cctp-bridge`.
///
/// The request carries the client's operation id, and each id holds at most
/// one burn (`CctpBurnOperation`, ADR 0023). The route signs the burn, records
/// the signed bytes under the id, then broadcasts them, so a retry with the
/// same id, even after a dropped request or a restart, reports the recorded
/// burn and sends the same bytes again instead of burning twice. A known id
/// whose burn is settled, or pending and already held by a node, is answered
/// before the gas check and the locks, since it sends nothing; a pending burn
/// no node holds is sent again under the lock and the pause, like a new one.
///
/// Like `complete_cctp_mint`, the recovery handle is checked before the
/// resume lock, and the lock and the driver pause are held around the work
/// that spends the rebalancing wallet's USDC, from the balance read through
/// the burn's confirmation. Both are taken inside the detached task, so they
/// stay held until the burn confirms even when the request is dropped.
///
/// The task answers the request at the broadcast, not at the receipt: an
/// approve plus the burn's confirmations can outlast the load balancer's 60
/// second cut. The task then awaits the receipt and records the outcome the
/// burn's own receipt proves.
pub(super) async fn cctp_bridge(
    State(state): State<AppState>,
    Audited(request): Audited<CctpBridgeRequest>,
) -> Result<Json<CctpBridgeResponse>, (StatusCode, Json<ErrorResponse>)> {
    let requested = burn_amount(request.amount.as_deref(), request.all)?;
    let operation_id = request.operation_id;
    let from = request.from;
    let direction = from.bridge_direction();
    let source_chain = from.chain();

    require_startup_complete(&state, "cctp-bridge")?;
    let handle = state.recovery.get().ok_or_else(recovery_not_ready)?;
    let required_confirmations = burn_required_confirmations(&state, source_chain)?;
    let wallets = bot_wallets(&state)?;
    let bridge = Arc::new(capital_cctp_bridge(&state, wallets)?);
    let store = Arc::clone(&handle.cctp_burn_store);
    let request = BurnRequest {
        operation_id,
        from,
        requested,
        required_confirmations,
    };

    // A settled burn, or a pending one a node already holds, sends nothing new,
    // so it is answered at once, without the gas check, the lock and the pause.
    // A pending burn no node holds must be sent again, which spends the
    // wallet's USDC, so it takes the locked path below.
    if let Some(recorded) = load_burn_operation(&store, &operation_id).await? {
        ensure_same_burn(&request, &recorded)?;
        if recorded.outcome.status() != CctpBurnStatus::Pending
            || burn_known_to_node(&bridge, &recorded).await
        {
            return Ok(Json(
                report_recorded_burn(&store, &bridge, &request, &recorded).await,
            ));
        }
    }

    // Every other corridor move proves both wallets can pay gas first: the
    // burn spends the source wallet's gas, and the `complete-mint` it leads to
    // spends the destination wallet's. The route only serves the Base and
    // Ethereum CCTP corridor, whose check is keyed by Base. Refused before
    // anything is locked.
    handle
        .rebalancing_service
        .ensure_usdc_corridor_gas_ready(UsdcCorridor::BASE_CCTP.chain())
        .await
        .map_err(|failure| {
            warn!(
                %failure,
                ?direction,
                "CCTP burn refused: the USDC corridor's signing wallets are not gas ready"
            );
            let (status, message) =
                usdc_resume_error_response(&UsdcResumeError::GasNotReady(failure));
            (status, Json(ErrorResponse { error: message }))
        })?;
    let corridor = state.ctx.rebalancing.cctp_corridor;
    let (source_wallet, source_usdc, recipient) = match from {
        CctpSourceChain::Ethereum => (
            Arc::clone(wallets.ethereum_wallet()),
            corridor.usdc_ethereum(),
            wallets.base_wallet().address(),
        ),
        CctpSourceChain::Base => (
            Arc::clone(wallets.base_wallet()),
            corridor.usdc_base(),
            wallets.ethereum_wallet().address(),
        ),
    };

    let resume_lock = Arc::clone(&state.resume_lock.0);
    let driver_pause = Arc::clone(&handle.usdc_driver_pause);
    answer_from_detached(
        &state.detached_tasks,
        "cctp-bridge",
        source_chain,
        async move {
            // The lock and the pause stay held until the burn confirms; the
            // confirmation future owns them.
            let held = (
                resume_lock.try_lock_owned().map_err(|_| {
                    (
                        StatusCode::CONFLICT,
                        Json(ErrorResponse {
                            error: "A resume or recheck operation is already in progress"
                                .to_string(),
                        }),
                    )
                })?,
                quiesce_usdc_driver(&driver_pause, UsdcDriverPauseRequest::CctpBurn { direction })
                    .await?,
            );

            // A request with the same id may have recorded its burn since the
            // lookup above; the lock serializes burns, so this read is final.
            if let Some(recorded) = load_burn_operation(&store, &operation_id).await? {
                return answer_recorded_burn_under_lock(store, bridge, request, &recorded, held)
                    .await;
            }

            let amount = match request.requested {
                RequestedBurn::Exact { amount } => amount,
                // Read under the driver pause, so no transfer spends the balance
                // between this read and the burn.
                RequestedBurn::All => {
                    let balance = source_wallet
                        .call::<OpenChainErrorRegistry, _>(
                            source_usdc,
                            IERC20::balanceOfCall {
                                account: source_wallet.address(),
                            },
                        )
                        .await
                        .map_err(|error| onchain_failure("cctp-bridge", source_chain, &error))?;
                    if balance.is_zero() {
                        warn!(%source_chain, "CCTP burn of the whole balance refused: it is zero");
                        return Err(ops_precondition_error(format!(
                            "the {source_chain} wallet's USDC balance is zero"
                        )));
                    }
                    balance
                }
            };

            // Signing broadcasts no burn; only a standing allowance approve
            // may have gone out, which moves no USDC.
            let prepared = bridge
                .prepare_burn(direction, amount, recipient)
                .boxed()
                .await
                .map_err(|error| onchain_failure("cctp-bridge", source_chain, &error))?;
            let burn_tx = prepared.tx_hash();
            let persisted = store
                .send(
                    &operation_id,
                    CctpBurnOperationCommand::Prepare {
                        source: from,
                        requested: request.requested,
                        amount,
                        recipient,
                        prepared: prepared.clone(),
                    },
                )
                .await;
            if let Err(error) = persisted {
                error!(%operation_id, %burn_tx, ?error, "Failed to record the signed CCTP burn; not broadcasting it");
                let recorded =
                    release_unpersisted_burn(&bridge, &store, &request, &prepared).await?;
                if recorded.burn_tx() != burn_tx {
                    return answer_recorded_burn_under_lock(
                        store, bridge, request, &recorded, held,
                    )
                    .await;
                }
            }

            broadcast_and_answer(store, bridge, request, &prepared, amount, held).await
        },
    )
    .await
    .map(Json)
}

/// What one `cctp-bridge` request asked for, as the adopt and confirm paths
/// need it.
#[derive(Clone, Copy)]
struct BurnRequest {
    operation_id: CctpBurnOperationId,
    from: CctpSourceChain,
    requested: RequestedBurn,
    required_confirmations: u64,
}

fn capital_cctp_bridge(
    state: &AppState,
    wallets: &OnchainWalletCtx,
) -> Result<BotCctpBridge, OpsError> {
    bot_cctp_bridge(&state.ctx, wallets).map_err(|error| {
        error!(?error, "Failed to build the CCTP bridge");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "Failed to build the CCTP bridge".to_string(),
            }),
        )
    })
}

/// The confirmation depth of the burn's source chain, which decides when its
/// receipt proves its fate.
fn burn_required_confirmations(state: &AppState, chain: Chain) -> Result<u64, OpsError> {
    state
        .ctx
        .chains
        .required_confirmations(chain)
        .ok_or_else(|| {
            error!(%chain, "CCTP burn refused: the source chain has no required_confirmations");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("{chain} has no [chains.{chain}] required_confirmations"),
                }),
            )
        })
}

async fn load_burn_operation(
    store: &Store<CctpBurnOperation>,
    id: &CctpBurnOperationId,
) -> Result<Option<CctpBurnOperation>, OpsError> {
    store.load(id).await.map_err(|error| {
        error!(operation_id = %id, ?error, "Could not load the CCTP burn operation");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!(
                    "Could not read operation {id}; nothing was burned by this request, retry \
                     with the same operation id"
                ),
            }),
        )
    })
}

/// Refuses a request that differs from the burn its operation id holds: the
/// id names one burn.
fn ensure_same_burn(request: &BurnRequest, recorded: &CctpBurnOperation) -> Result<(), OpsError> {
    if (recorded.source, recorded.requested) == (request.from, request.requested) {
        return Ok(());
    }
    let operation_id = request.operation_id;
    let burn_tx = recorded.burn_tx();
    warn!(
        %operation_id, %burn_tx, recorded_source = ?recorded.source,
        recorded_request = ?recorded.requested, source = ?request.from,
        requested = ?request.requested,
        "CCTP burn refused: the operation id already holds a different burn"
    );
    Err((
        StatusCode::CONFLICT,
        Json(ErrorResponse {
            error: format!(
                "Operation id {operation_id} already holds burn {burn_tx} of {} from {}; use a \
                 new operation id for a different burn",
                describe_requested_burn(recorded.requested),
                recorded.source.chain()
            ),
        }),
    ))
}

/// Answers with the burn an operation id holds, sending nothing: a settled
/// burn reports its outcome (and releases a taken nonce again, idempotent),
/// and a pending one reports what its own receipt now proves.
async fn report_recorded_burn(
    store: &Store<CctpBurnOperation>,
    bridge: &BotCctpBridge,
    request: &BurnRequest,
    recorded: &CctpBurnOperation,
) -> CctpBridgeResponse {
    let operation_id = request.operation_id;
    if recorded.outcome.status() != CctpBurnStatus::Pending {
        release_settled_burn_nonce(bridge, recorded).await;
        return CctpBridgeResponse::of(operation_id, recorded, recorded.outcome.status());
    }
    let status = settle_burn_from_receipt(
        store,
        bridge,
        &operation_id,
        recorded,
        request.required_confirmations,
    )
    .await;
    info!(%operation_id, burn_tx = %recorded.burn_tx(), ?status, "CCTP burn reported via API: the operation id already holds this burn");
    CctpBridgeResponse::of(operation_id, recorded, status)
}

/// Whether a node already holds the pending burn, mined or in its mempool, so
/// a rerun has nothing to send. A failed or lagging read counts as unknown:
/// the rerun then sends the same bytes under the lock and the pause.
async fn burn_known_to_node(bridge: &BotCctpBridge, recorded: &CctpBurnOperation) -> bool {
    matches!(
        bridge
            .source_knows_tx(recorded.source.bridge_direction(), recorded.burn_tx())
            .boxed()
            .await,
        Ok(true)
    )
}

/// Answers, under the resume lock and the driver pause, a request whose
/// operation id already holds a burn: a settled one is reported, and a
/// pending one is sent again from its recorded bytes, like a fresh burn, since
/// sending spends the wallet's USDC.
async fn answer_recorded_burn_under_lock<Held: Send + 'static>(
    store: Arc<Store<CctpBurnOperation>>,
    bridge: Arc<BotCctpBridge>,
    request: BurnRequest,
    recorded: &CctpBurnOperation,
    held: Held,
) -> Result<(CctpBridgeResponse, BoxFuture<'static, ()>), OpsError> {
    ensure_same_burn(&request, recorded)?;
    if recorded.outcome.status() != CctpBurnStatus::Pending {
        let response = report_recorded_burn(&store, &bridge, &request, recorded).await;
        return Ok((response, std::future::ready(()).boxed()));
    }
    broadcast_and_answer(
        store,
        bridge,
        request,
        &recorded.prepared,
        recorded.amount,
        held,
    )
    .await
}

/// Broadcasts a recorded burn's signed bytes while the caller holds the
/// resume lock and the driver pause in `held`, and answers with the burn as
/// pending; the returned future confirms it and holds `held` until then.
/// After every broadcast attempt the operation is reloaded: the supersede
/// route may have settled it meanwhile, and a broadcast after its nonce
/// release books the nonce again (even an accepted one, from a node that has
/// not seen the other tx), so a settled burn releases it again and reports its
/// outcome. A failed broadcast also reports what the burn's own receipt
/// proves, and answers `502` only while it is still pending, so the operator
/// never takes an unsent burn for one in flight. A burn signed by another
/// address than the source wallet (a rotated key) is never broadcast through
/// this wallet, whose nonce bookkeeping is per address.
async fn broadcast_and_answer<Held: Send + 'static>(
    store: Arc<Store<CctpBurnOperation>>,
    bridge: Arc<BotCctpBridge>,
    request: BurnRequest,
    prepared: &PreparedTransaction,
    amount: U256,
    held: Held,
) -> Result<(CctpBridgeResponse, BoxFuture<'static, ()>), OpsError> {
    let operation_id = request.operation_id;
    let direction = request.from.bridge_direction();
    let burn_tx = prepared.tx_hash();
    let wallet = bridge.source_signer(direction);
    let failure = if prepared.signer() == Some(wallet) {
        bridge
            .broadcast_prepared_burn(direction, prepared)
            .boxed()
            .await
            .err()
            .map(|error| {
                // The error's text can carry the RPC URL, whose path or query
                // holds the provider key, so it is logged scrubbed and never
                // put in the answer or the alert.
                error!(
                    %operation_id, %burn_tx, ?direction,
                    error = %scrubbed(&error),
                    "Broadcasting the recorded CCTP burn failed"
                );
                "its broadcast failed (see the bot logs); rerun with the same operation id to \
                 broadcast the same burn again, or, if another tx took its nonce, settle it with \
                 cctp-burn-supersede"
                    .to_string()
            })
    } else {
        Some(format!(
            "it was signed by {:?}, not this bot's wallet {wallet} (was the key rotated?), so \
             this bot never broadcasts it; check it onchain",
            prepared.signer()
        ))
    };
    // Never fatal: after an accepted broadcast the burn is live, so a failed
    // read must not drop `held` or claim nothing burned. Without the reload,
    // an accepted burn is answered pending and confirmed as usual, and a
    // failed one gets its `502`.
    let current = store.load(&operation_id).await.unwrap_or_else(|error| {
        warn!(%operation_id, %burn_tx, ?error, "Could not reload the CCTP burn operation after its broadcast attempt; answering from the attempt");
        None
    });
    if let Some(current) = &current
        && current.outcome.status() != CctpBurnStatus::Pending
    {
        release_settled_burn_nonce(&bridge, current).await;
        let response = CctpBridgeResponse::of(operation_id, current, current.outcome.status());
        return Ok((response, std::future::ready(()).boxed()));
    }
    if let Some(failure) = failure {
        if let Some(current) = &current {
            let response = report_recorded_burn(&store, &bridge, &request, current).await;
            if response.status != CctpBurnStatus::Pending {
                return Ok((response, std::future::ready(()).boxed()));
            }
        }
        error!(target: "operational_alert", alert = true, kind = AlertKind::CapitalCctpBurnSendUnproven.as_str(), %operation_id, %burn_tx, ?direction, %failure, "A recorded CCTP burn could not be sent, and its receipt does not prove its outcome");
        return Err((
            StatusCode::BAD_GATEWAY,
            Json(ErrorResponse {
                error: format!(
                    "Burn {burn_tx} is recorded under operation id {operation_id}, but \
                     {failure}"
                ),
            }),
        ));
    }
    info!(%operation_id, %burn_tx, ?direction, %amount, "CCTP burn broadcast via API");

    let response = CctpBridgeResponse {
        operation_id,
        burn_tx,
        source_chain: request.from.chain(),
        destination_chain: request.from.destination_chain(),
        amount_raw: amount.to_string(),
        status: CctpBurnStatus::Pending,
    };
    let confirm = async move {
        // The lock and the pause stay held until the burn confirms.
        let _held = held;
        confirm_recorded_burn(&store, &bridge, &request, burn_tx, amount).await;
    };
    Ok((response, confirm.boxed()))
}

/// Releases the source wallet's hold on the nonce of a burn another tx took
/// (`Superseded` or `Replaced`). Idempotent, so a stale broadcast that booked
/// the nonce again after an earlier release is cleared too. A burn signed by
/// another wallet holds no nonce here.
async fn release_settled_burn_nonce(bridge: &BotCctpBridge, operation: &CctpBurnOperation) {
    let nonce_taken = matches!(
        operation.outcome.status(),
        CctpBurnStatus::Superseded | CctpBurnStatus::Replaced
    );
    if nonce_taken && signed_by_source_wallet(bridge, operation) {
        bridge
            .release_superseded_burn(operation.source.bridge_direction(), &operation.prepared)
            .boxed()
            .await;
    }
}

fn describe_requested_burn(requested: RequestedBurn) -> String {
    match requested {
        RequestedBurn::Exact { amount } => format!("{amount} USDC base units"),
        RequestedBurn::All => "the whole balance".to_string(),
    }
}

/// After a failed `Prepare` write, reloads the operation. These bytes
/// recorded (the write committed): returns them, and the route broadcasts.
/// Another burn recorded under the id: releases this signature's nonce and
/// returns the recorded burn to adopt. Nothing recorded: releases the nonce,
/// since nothing will broadcast it. A failed reload cannot tell, so the nonce
/// stays reserved until a restart, as for the deposit send: a rerun before
/// then could sign behind it. Mirrors `release_unpersisted_deposit_send`.
async fn release_unpersisted_burn(
    bridge: &BotCctpBridge,
    store: &Store<CctpBurnOperation>,
    request: &BurnRequest,
    prepared: &PreparedTransaction,
) -> Result<CctpBurnOperation, OpsError> {
    let operation_id = request.operation_id;
    let direction = request.from.bridge_direction();
    let burn_tx = prepared.tx_hash();
    let nonce = prepared.nonce();
    match store.load(&operation_id).await {
        Ok(Some(recorded)) if recorded.burn_tx() == burn_tx => {
            warn!(%operation_id, %burn_tx, "The failed CCTP burn write committed; broadcasting it");
            Ok(recorded)
        }
        Ok(Some(recorded)) => {
            warn!(%operation_id, %burn_tx, nonce, recorded = %recorded.burn_tx(), "Releasing the nonce of a signed CCTP burn: the operation id already holds another burn");
            bridge
                .discard_prepared_burn(direction, prepared)
                .boxed()
                .await;
            Ok(recorded)
        }
        Ok(None) => {
            warn!(%operation_id, %burn_tx, nonce, "Releasing the nonce of a signed CCTP burn that was not recorded");
            bridge
                .discard_prepared_burn(direction, prepared)
                .boxed()
                .await;
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!(
                        "Could not record the signed burn under operation id {operation_id}; \
                         nothing was broadcast, so a rerun with the same operation id burns \
                         once"
                    ),
                }),
            ))
        }
        Err(error) => {
            error!(target: "operational_alert", alert = true, kind = AlertKind::CapitalCctpBurnRecordUnknown.as_str(), %operation_id, %burn_tx, nonce, ?error, "Cannot tell whether a signed CCTP burn was recorded; its nonce stays reserved and every later send from the source wallet waits behind it until a restart");
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!(
                        "Could not tell whether burn {burn_tx} was recorded under operation id \
                         {operation_id}; nothing was broadcast, and its nonce stays reserved \
                         until the bot restarts. Rerun with the same operation id only after \
                         a restart: it then broadcasts the burn if it was recorded, or burns \
                         once if not"
                    ),
                }),
            ))
        }
    }
}

/// Awaits the broadcast burn's receipt, then records the outcome a fresh read
/// of its own receipt proves at the required confirmations. The wait alone
/// does not decide: it keeps the first receipt it saw while it counts blocks,
/// so a shallow reorg can remove the burn's block without it noticing, and
/// the record is the one source of truth for the id. Anything the fresh read
/// does not decide (a timeout, a drop report, an RPC error, a shallow or
/// missing receipt) records nothing: the burn stays pending, and a rerun with
/// its operation id reports it and broadcasts it again.
async fn confirm_recorded_burn(
    store: &Store<CctpBurnOperation>,
    bridge: &BotCctpBridge,
    request: &BurnRequest,
    burn_tx: TxHash,
    amount: U256,
) {
    let operation_id = request.operation_id;
    let direction = request.from.bridge_direction();
    let error = bridge
        .confirm_burn(direction, burn_tx, amount)
        .await
        .err()
        .map(|error| scrubbed(&error));
    let fate = match burn_receipt_fate(
        bridge,
        request.from,
        burn_tx,
        request.required_confirmations,
    )
    .await
    {
        Ok(Some(fate)) => fate,
        read => {
            warn!(
                %operation_id, %burn_tx, ?direction, %amount, ?error,
                read = ?read.map_err(|error| scrubbed(&error)),
                "CCTP burn broadcast via API is not confirmed yet; a cctp-bridge rerun with its \
                 operation id reports its status and broadcasts it again"
            );
            return;
        }
    };
    match fate {
        BurnReceiptFate::Confirmed => info!(
            %operation_id, %burn_tx, ?direction, %amount, ?error,
            "CCTP burn confirmed via API"
        ),
        BurnReceiptFate::Reverted => error!(
            %operation_id, %burn_tx, ?direction, %amount, ?error,
            "CCTP burn broadcast via API reverted; it burned nothing, and a new operation id \
             burns again"
        ),
    }
    record_burn_fate(store, &operation_id, burn_tx, fate).await;
}

/// Settles a pending capital CCTP burn that can never mine because another
/// tx from its source wallet took its nonce. The operator names that tx: a
/// plain 0 value self transfer that cancelled the burn, or a fee bumped copy
/// of the burn (a wallet's speed up) that burned in its place. The checks
/// mirror `transfer reconcile --superseding-tx` and `adopt-withdrawal` for a
/// signed vault withdrawal (`verify_burn_superseded`). The operation records
/// the burn `Superseded` or `Replaced`, and the source wallet releases the
/// burn's nonce, so startup stops restoring it. A burn whose outcome is
/// already recorded, or whose own receipt now decides, reports that outcome
/// instead, and a settled one releases the nonce again (idempotent).
pub(super) async fn cctp_burn_supersede(
    State(state): State<AppState>,
    Audited(request): Audited<CctpBurnSupersedeRequest>,
) -> Result<Json<CctpBridgeResponse>, OpsError> {
    let CctpBurnSupersedeRequest {
        operation_id,
        superseding_tx,
    } = request;
    require_startup_complete(&state, "cctp-burn-supersede")?;
    let handle = state.recovery.get().ok_or_else(recovery_not_ready)?;
    let store = &handle.cctp_burn_store;
    let Some(recorded) = load_burn_operation(store, &operation_id).await? else {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("No burn is recorded under operation id {operation_id}"),
            }),
        ));
    };
    let source = recorded.source;
    let required_confirmations = burn_required_confirmations(&state, source.chain())?;
    let wallets = bot_wallets(&state)?;
    let bridge = capital_cctp_bridge(&state, wallets)?;

    let status = settle_burn_from_receipt(
        store,
        &bridge,
        &operation_id,
        &recorded,
        required_confirmations,
    )
    .await;
    if status != CctpBurnStatus::Pending {
        let current = load_burn_operation(store, &operation_id)
            .await?
            .unwrap_or(recorded);
        release_settled_burn_nonce(&bridge, &current).await;
        return Ok(Json(CctpBridgeResponse::of(operation_id, &current, status)));
    }

    let taken_by = verify_burn_superseded(
        &bridge,
        source,
        &recorded.prepared,
        superseding_tx,
        required_confirmations,
    )
    .await
    .map_err(|error| {
        // `Display`, not `Debug`: a read error's `Debug` names the RPC URL,
        // whose path carries the key.
        warn!(%error, %operation_id, "Refused to settle a CCTP burn as superseded");
        let status = match error {
            BurnNotSuperseded::Read { .. } => StatusCode::BAD_GATEWAY,
            BurnNotSuperseded::UnreadableBurn { .. }
            | BurnNotSuperseded::BurnMined { .. }
            | BurnNotSuperseded::SupersedingTxIsTheBurn { .. }
            | BurnNotSuperseded::SupersedingTxNotMined { .. }
            | BurnNotSuperseded::SupersedingTxFromAnotherSender { .. }
            | BurnNotSuperseded::SupersedingTxAtAnotherNonce { .. }
            | BurnNotSuperseded::SupersedingTxUnconfirmed { .. }
            | BurnNotSuperseded::SupersedingTxNotAPlainCancel { .. } => StatusCode::CONFLICT,
        };
        (
            status,
            Json(ErrorResponse {
                error: format!(
                    "Operation {operation_id}: refusing to settle the burn as superseded: {error}"
                ),
            }),
        )
    })?;

    let command = match taken_by {
        NonceTakenBy::Cancel => CctpBurnOperationCommand::RecordSuperseded { superseding_tx },
        NonceTakenBy::Replacement => CctpBurnOperationCommand::RecordReplaced {
            replacement_tx: superseding_tx,
        },
    };
    store
        .send(&operation_id, command)
        .await
        .map_err(ops_command_error)?;
    let settled = load_burn_operation(store, &operation_id)
        .await?
        .unwrap_or(recorded);
    release_settled_burn_nonce(&bridge, &settled).await;
    info!(
        %operation_id, burn_tx = %settled.burn_tx(), %superseding_tx, ?taken_by,
        "CCTP burn settled via API: another tx took its nonce"
    );
    Ok(Json(CctpBridgeResponse::of(
        operation_id,
        &settled,
        settled.outcome.status(),
    )))
}

/// Zeroes the chosen chain's settlement stable allowance for that chain's
/// orderbook through `RaindexService::submit_revoke_orderbook_allowance`, the
/// broadcast half of the startup revoke. Mirrors `st0x-cli reset-allowance`.
/// Like the vault routes, it answers at the broadcast and confirms the revoke
/// on the detached task.
pub(super) async fn reset_allowance(
    State(state): State<AppState>,
    Audited(request): Audited<ResetAllowanceRequest>,
) -> Result<Json<ResetAllowanceResponse>, (StatusCode, Json<ErrorResponse>)> {
    let ResetAllowanceRequest { chain } = request;
    let (trading, wallet) = hedged_chain_signer(&state, chain)?;
    require_startup_complete(&state, "reset-allowance")?;
    let token = chain.settlement_stable().address;
    let contracts = crate::onchain::raindex_contracts(trading);
    let spender = contracts.orderbook;
    let owner = wallet.address();
    let raindex = RaindexService::new(wallet, contracts, owner);

    let outcome = answer_from_detached(&state.detached_tasks, "reset-allowance", chain, async move {
        let outcome = raindex
            .submit_revoke_orderbook_allowance::<OpenChainErrorRegistry>(token)
            .await
            .map_err(|error| onchain_failure("reset-allowance", chain, &error))?;
        info!(%chain, %token, %spender, ?outcome, "Orderbook allowance reset via API");

        let confirm = async move {
            let RevokeOutcome::Revoked { tx } = outcome else {
                return;
            };
            match raindex.confirm_tx(tx).await {
                Ok(()) => {
                    info!(%chain, %token, %spender, %tx, "Orderbook allowance reset confirmed via API");
                }
                Err(error) => error!(
                    %chain, %token, %spender, %tx, ?error,
                    "Orderbook allowance reset via API did not confirm; check the tx onchain \
                     before retrying"
                ),
            }
        };
        Ok::<_, OpsError>((outcome, confirm))
    })
    .await?;

    let (outcome, tx) = match outcome {
        RevokeOutcome::Revoked { tx } => (ResetAllowanceOutcome::Revoked, Some(tx)),
        RevokeOutcome::AlreadyZero => (ResetAllowanceOutcome::AlreadyZero, None),
    };
    Ok(Json(ResetAllowanceResponse {
        chain,
        token,
        spender,
        outcome,
        tx,
    }))
}

/// Deposits `quantity` underlying shares of `symbol` into the chain's ERC-4626
/// wrapper from the bot's wallet there, which receives the wrapped shares.
/// Mirrors `st0x-cli wrap-equity`, signed by the bot's own wallet; see
/// [`run_wrapper_operation`].
pub(super) async fn wrap_equity(
    State(state): State<AppState>,
    Audited(request): Audited<WrapRequest>,
) -> Result<Json<WrapResponse>, OpsError> {
    let WrapRequest {
        symbol,
        quantity,
        chain,
    } = request;
    let (symbol, quantity, amount) = equity_wallet_input(&symbol, &quantity)?;
    let wrap_tx =
        run_wrapper_operation(&state, WrapperOperation::Wrap, chain, &symbol, amount).await?;

    Ok(Json(WrapResponse {
        chain,
        symbol: symbol.to_string(),
        quantity: quantity.inner().to_string(),
        wrap_tx,
    }))
}

/// Redeems `quantity` wrapped shares of `symbol` from the chain's ERC-4626
/// wrapper, owned by and paid out to the bot's wallet there. Mirrors
/// `st0x-cli unwrap-equity`, signed by the bot's own wallet; see
/// [`run_wrapper_operation`].
pub(super) async fn unwrap_equity(
    State(state): State<AppState>,
    Audited(request): Audited<WrapRequest>,
) -> Result<Json<UnwrapResponse>, OpsError> {
    let WrapRequest {
        symbol,
        quantity,
        chain,
    } = request;
    let (symbol, quantity, amount) = equity_wallet_input(&symbol, &quantity)?;
    let unwrap_tx =
        run_wrapper_operation(&state, WrapperOperation::Unwrap, chain, &symbol, amount).await?;

    Ok(Json(UnwrapResponse {
        chain,
        symbol: symbol.to_string(),
        quantity: quantity.inner().to_string(),
        unwrap_tx,
    }))
}

/// Which wrapper operation a wrap route runs; each names its route.
#[derive(Clone, Copy)]
enum WrapperOperation {
    Wrap,
    Unwrap,
}

impl WrapperOperation {
    const fn route(self) -> &'static str {
        match self {
            Self::Wrap => "wrap-equity",
            Self::Unwrap => "unwrap-equity",
        }
    }
}

/// Wraps or unwraps `amount` (18 decimals) of `symbol` on `chain` through the
/// chain's wrapper, built on the bot's signer from the chain's asset table as
/// the conductor builds its own, so the route shares the bot's nonce state.
///
/// Ordered like the vault routes: input and config checks, the startup gate,
/// then a detached task that holds the symbol's lock and the bot's equity
/// transfer guard (`claim_equity_wallet`) from before its first read until
/// the tx's fate is proven, checks the signer's gas, and sends. A wrap first
/// approves the wrapper when its allowance is short and awaits that approve,
/// as `Wrapper::submit_wrap` does for the bot's own wraps; the deposit or
/// redeem itself is answered at its broadcast, since the chain's required
/// confirmations outlast the load balancer's 60 second cut. The task then
/// awaits the confirmation through `Wrapper::confirm_wrap` or
/// `Wrapper::confirm_unwrap`, waits again while the outcome is unknown, and
/// logs what the wallet received. A failed send frees the claim, like
/// `st0x-cli`: its error cannot tell whether the tx went out, which
/// `onchain_failure` tells the operator to check before retrying.
async fn run_wrapper_operation(
    state: &AppState,
    operation: WrapperOperation,
    chain: Chain,
    symbol: &Symbol,
    amount: U256,
) -> Result<TxHash, OpsError> {
    let route = operation.route();
    let (trading, wallet) = equity_chain_signer(state, chain, symbol)?;
    require_startup_complete(state, route)?;
    let handle = state.recovery.get().ok_or_else(recovery_not_ready)?;
    let rebalancing = Arc::clone(&handle.rebalancing_service);
    let locks = Arc::clone(&state.equity_wallet_locks);
    // Checked under the claim, with the balance read.
    let gas = signer_gas_readiness(state, chain, &wallet);
    let required_confirmations = trading.required_confirmations;
    let owner = wallet.address();
    let wrapper = WrapperService::new(
        Arc::clone(&wallet),
        to_wrapped_equities(&trading.assets.equities.symbols),
    );
    let wrapped_token = wrapper
        .lookup_derivative(symbol)
        .map_err(|error| unprocessable(error.to_string()))?;
    let symbol = symbol.clone();

    answer_from_detached(&state.detached_tasks, route, symbol.clone(), async move {
        let claim = claim_equity_wallet(&locks, &rebalancing, &symbol, chain, route)?;
        ensure_signer_gas(&*gas?, route, chain).await?;

        let submitted = match operation {
            WrapperOperation::Wrap => wrapper.submit_wrap(wrapped_token, amount, owner).await,
            WrapperOperation::Unwrap => {
                wrapper
                    .submit_unwrap(wrapped_token, amount, owner, owner)
                    .await
            }
        };
        let tx = submitted.map_err(|error| wrapper_submit_failure(route, chain, &error))?;
        info!(
            route, %chain, %symbol, %wrapped_token, %amount, %tx,
            "Equity wallet operation broadcast via API"
        );

        let confirm = async move {
            let wait = confirm_wrapper_tx(
                &wrapper,
                operation,
                wrapped_token,
                amount,
                PendingEquityTx {
                    wallet: &wallet,
                    chain,
                    symbol: &symbol,
                    tx,
                    required_confirmations,
                },
            );
            hold_until_proven(claim, route, &symbol, tx, wait).await;
        };
        Ok::<_, OpsError>((tx, confirm))
    })
    .await
}

/// One tx the bot sent for an operator while holding a symbol's claim, as a
/// confirmation loop needs it.
#[derive(Clone, Copy)]
struct PendingEquityTx<'pending> {
    wallet: &'pending Arc<dyn Wallet<Provider = RootProvider>>,
    chain: Chain,
    symbol: &'pending Symbol,
    tx: TxHash,
    required_confirmations: u64,
}

/// What a wrap or unwrap confirmation proved.
enum WrapperFate {
    /// Confirmed, with what the wallet received: wrapped shares for a wrap,
    /// underlying for an unwrap.
    Received(U256),
    /// The tx's receipt proves it succeeded, but the wrapper could not read
    /// what it paid out.
    ConfirmedUnaccounted,
    Failed,
}

/// Awaits a wrap or unwrap tx's confirmation through `Wrapper::confirm_wrap`
/// or `Wrapper::confirm_unwrap`, waits again while its fate is unknown, and
/// logs what the wallet received. The caller holds the symbol's claim, so a
/// fate that stays unknown alerts once ([`UnprovenCapitalTx`]).
async fn confirm_wrapper_tx(
    wrapper: &impl Wrapper,
    operation: WrapperOperation,
    wrapped_token: Address,
    amount: U256,
    pending: PendingEquityTx<'_>,
) -> WrapperFate {
    let PendingEquityTx {
        wallet,
        chain,
        symbol,
        tx,
        required_confirmations,
    } = pending;
    let route = operation.route();
    let mut unproven = UnprovenCapitalTx::new(tokio::time::Instant::now());
    loop {
        let confirmed = match operation {
            WrapperOperation::Wrap => wrapper
                .confirm_wrap(wrapped_token, tx)
                .await
                .map(|confirmation| confirmation.shares),
            WrapperOperation::Unwrap => wrapper
                .confirm_unwrap(wrapped_token, tx)
                .await
                .map(|confirmation| confirmation.assets),
        };
        let error = match confirmed {
            Ok(received) => {
                info!(
                    route, %chain, %symbol, %amount, %received, %tx,
                    "Equity wallet operation confirmed via API"
                );
                return WrapperFate::Received(received);
            }
            Err(error) => error,
        };
        let mined = mined_or_unknown(wallet, route, chain, tx).await;
        match wrapper_tx_fate(&error, mined.as_ref(), required_confirmations) {
            TxFate::Confirmed => {
                info!(
                    route, %chain, %symbol, %amount, %tx, ?error,
                    "Equity wallet operation confirmed via API"
                );
                return WrapperFate::ConfirmedUnaccounted;
            }
            TxFate::Failed => {
                error!(
                    route, %chain, %symbol, %amount, %tx, ?error,
                    "Equity wallet operation broadcast via API did not confirm"
                );
                return WrapperFate::Failed;
            }
            TxFate::Unknown => {
                warn!(
                    route, %chain, %symbol, %amount, %tx, ?error,
                    "Equity wallet operation broadcast via API is not confirmed yet; keeping \
                     its lock and waiting again"
                );
                unproven.observe(tokio::time::Instant::now(), route, chain, symbol, tx);
                tokio::time::sleep(CONFIRM_RETRY_DELAY).await;
            }
        }
    }
}

/// A refused wrap or unwrap send. An unwrap above the wrapper's `maxRedeem`
/// for the bot's wallet is refused before any tx, so it is a 422 naming what
/// the wallet can redeem; anything else may have reached the chain, so it is
/// the generic onchain 500.
fn wrapper_submit_failure(route: &'static str, chain: Chain, error: &WrapperError) -> OpsError {
    if let WrapperError::RedeemExceedsMax { .. } = error {
        warn!(route, %chain, %error, "Equity wallet operation refused before its send");
        return unprocessable(format!("{route} refused: {error}"));
    }
    onchain_failure(route, chain, error)
}

/// The tx's own canonical receipt, or `None` when it cannot be read, which
/// proves nothing either way.
async fn mined_or_unknown(
    wallet: &Arc<dyn Wallet<Provider = RootProvider>>,
    route: &'static str,
    chain: Chain,
    tx: TxHash,
) -> Option<MinedTx> {
    st0x_evm::mined_tx(wallet.provider(), tx)
        .await
        .unwrap_or_else(|read_error| {
            warn!(route, %chain, %tx, ?read_error, "Could not read the tx's receipt");
            None
        })
}

/// The tokenization client a redemption sends and polls through.
type RedemptionTokenizer = AlpacaTokenizationService<Arc<dyn Wallet<Provider = RootProvider>>>;

/// Sends `quantity` shares of `symbol`'s underlying tokenized equity from the
/// bot's wallet on `chain` to Alpaca's redemption wallet there, and Alpaca
/// redeems them into the account. Mirrors `st0x-cli alpaca-redeem`, kept to
/// drain a paused chain, where `transfer-equity` starts no new transfer.
///
/// The token is the chain's wrapper `asset()`, attested like the CLI does
/// (`Wrapper::attest_underlying`), never a pasted address. The redemption
/// wallet is always the chain's own
/// `[chains.<name>.trading].redemption_wallet`, never one the request names:
/// a wallet chosen by the caller would let anyone in the capital group send
/// the bot's shares out of our accounts. The tokenization client is built from the
/// bot's broker ctx and signer as the conductor builds its own.
///
/// Ordered and claimed like the wrap routes. The transfer is sent once and
/// never retried: it carries no idempotency key, so a retry could send the
/// shares twice (RAI-1494). The route answers at its broadcast; the detached
/// task confirms it, holding the claim until its fate is proven, then polls
/// Alpaca until it detects the transfer and until the redemption finishes,
/// and logs the request id and the final status. Neither poll is awaited by
/// the request, since both can outlast the load balancer's 60 second cut.
pub(super) async fn redeem(
    State(state): State<AppState>,
    Audited(request): Audited<RedeemRequest>,
) -> Result<Json<RedeemResponse>, OpsError> {
    let RedeemRequest {
        symbol,
        quantity,
        chain,
    } = request;
    let (symbol, quantity, amount) = equity_wallet_input(&symbol, &quantity)?;
    let (trading, wallet) = equity_chain_signer(&state, chain, &symbol)?;
    let redemption_wallet = configured_redemption_wallet(&state, chain)?;
    require_startup_complete(&state, "redeem")?;
    let handle = state.recovery.get().ok_or_else(recovery_not_ready)?;
    let rebalancing = Arc::clone(&handle.rebalancing_service);
    let locks = Arc::clone(&state.equity_wallet_locks);
    // Checked under the claim, with the balance read.
    let gas = signer_gas_readiness(&state, chain, &wallet);
    let required_confirmations = trading.required_confirmations;
    let wrapper = WrapperService::new(
        Arc::clone(&wallet),
        to_wrapped_equities(&trading.assets.equities.symbols),
    );
    let tokenizer = redemption_tokenizer(&state, chain, &wallet, redemption_wallet)?;
    let task_symbol = symbol.clone();

    let (token, transfer_tx) = answer_from_detached(
        &state.detached_tasks,
        "redeem",
        symbol.clone(),
        async move {
            let symbol = task_symbol;
            let claim = claim_equity_wallet(&locks, &rebalancing, &symbol, chain, "redeem")?;
            ensure_signer_gas(&*gas?, "redeem", chain).await?;

            let token = wrapper
                .attest_underlying(&symbol)
                .await
                .map_err(|error| match error {
                    WrapperError::VaultAssetMismatch { .. } => {
                        warn!(%chain, %symbol, %error, "Redemption refused before its send");
                        unprocessable(format!("redeem refused: {error}"))
                    }
                    other => onchain_failure("redeem", chain, &other),
                })?;

            // Sent exactly once: the transfer carries no idempotency key
            // (unlike a mint's issuer request id), so a retry could send the
            // shares twice (RAI-1494).
            let transfer_tx = tokenizer
                .submit_redemption_send(token, amount)
                .await
                .map_err(|error| onchain_failure("redeem", chain, &error))?;
            info!(
                %chain, %symbol, %token, %redemption_wallet, %amount, %transfer_tx,
                "Redemption transfer broadcast via API"
            );

            let follow = async move {
                let wait = confirm_redemption_transfer(
                    &tokenizer,
                    &wallet,
                    &symbol,
                    chain,
                    transfer_tx,
                    required_confirmations,
                );
                if hold_until_proven(claim, "redeem", &symbol, transfer_tx, wait).await {
                    follow_alpaca_redemption(&tokenizer, &symbol, chain, transfer_tx).await;
                }
            };
            Ok::<_, OpsError>(((token, transfer_tx), follow))
        },
    )
    .await?;

    Ok(Json(RedeemResponse {
        chain,
        symbol: symbol.to_string(),
        quantity: quantity.inner().to_string(),
        redemption_wallet,
        token: token.address(),
        transfer_tx,
    }))
}

/// Drains `quantity` wrapped shares of `symbol` from a paused chain into
/// Alpaca in one run: withdraws them from the vault to the bot's wallet,
/// unwraps them, and sends the underlying the unwrap paid out to the chain's
/// configured Alpaca redemption wallet, which Alpaca redeems into the account.
///
/// The three steps run under one claim of the symbol (its lock and the bot's
/// equity transfer guard), held from before the withdrawal until the
/// redemption transfer's fate is proven. Separately, `vault-withdraw`,
/// `unwrap-equity` and `redeem` free the guard between steps, and the bot's
/// wallet recovery, which waits while the guard is held, could deposit the
/// shares back into the vault before the next step. The drain also holds the
/// vault withdraw lock until its withdrawal's fate is proven, like
/// `vault-withdraw`.
///
/// The token checks come before anything moves: the wrapped token is the
/// listing's own, and its wrapper must attest the configured underlying. The
/// route answers at the withdrawal's broadcast; the rest runs on the detached
/// task, each step after the previous one is proven. A step that fails stops
/// the drain where it is and logs where the shares are; once the claim is
/// freed the bot's wallet recovery may deposit them back. The redemption
/// transfer is sent once and never retried, like `redeem`, and redeems what
/// the unwrap paid out; when the unwrap's receipt proves success but its
/// payout cannot be read, the drain stops before the transfer.
pub(super) async fn drain_equity(
    State(state): State<AppState>,
    Audited(request): Audited<DrainEquityRequest>,
) -> Result<Json<DrainEquityResponse>, OpsError> {
    const ROUTE: &str = "drain-equity";
    let DrainEquityRequest {
        symbol,
        quantity,
        chain,
        vault_id,
    } = request;
    let (symbol, quantity, amount) = equity_wallet_input(&symbol, &quantity)?;
    let (trading, wallet) = equity_chain_signer(&state, chain, &symbol)?;
    let redemption_wallet = configured_redemption_wallet(&state, chain)?;
    require_startup_complete(&state, ROUTE)?;
    let handle = state.recovery.get().ok_or_else(recovery_not_ready)?;
    let rebalancing = Arc::clone(&handle.rebalancing_service);
    let locks = Arc::clone(&state.equity_wallet_locks);
    let withdraw_lock = Arc::clone(&state.vault_withdraw_lock);
    // Checked under the claim, with the balance read.
    let gas = signer_gas_readiness(&state, chain, &wallet);
    let required_confirmations = trading.required_confirmations;
    let owner = wallet.address();
    let wrapper = WrapperService::new(
        Arc::clone(&wallet),
        to_wrapped_equities(&trading.assets.equities.symbols),
    );
    let wrapped_token = wrapper
        .lookup_derivative(&symbol)
        .map_err(|error| unprocessable(error.to_string()))?;
    let raindex = RaindexService::new(
        Arc::clone(&wallet),
        crate::onchain::raindex_contracts(trading),
        owner,
    );
    let tokenizer = redemption_tokenizer(&state, chain, &wallet, redemption_wallet)?;
    let task_symbol = symbol.clone();

    let withdraw_tx = answer_from_detached(&state.detached_tasks, ROUTE, symbol.clone(), async move {
        let symbol = task_symbol;
        let claim = claim_equity_wallet(&locks, &rebalancing, &symbol, chain, ROUTE)?;
        let withdraw_guard = withdraw_lock.try_lock_owned().map_err(|_| {
            warn!(%chain, %symbol, "Drain refused: another vault withdrawal holds the withdraw lock");
            conflict(
                "Another vault withdrawal is in progress or its outcome is unknown; check the \
                 bot logs for it before retrying"
                    .to_string(),
            )
        })?;
        ensure_signer_gas(&*gas?, ROUTE, chain).await?;
        let token = wrapper
            .attest_underlying(&symbol)
            .await
            .map_err(|error| match error {
                WrapperError::VaultAssetMismatch { .. } => {
                    warn!(%chain, %symbol, %error, "Drain refused before its withdrawal");
                    unprocessable(format!("{ROUTE} refused: {error}"))
                }
                other => onchain_failure(ROUTE, chain, &other),
            })?;
        let decimals = wallet
            .call::<OpenChainErrorRegistry, _>(wrapped_token, IERC20::decimalsCall {})
            .await
            .map_err(|error| onchain_failure(ROUTE, chain, &error))?;
        if decimals != 18 {
            error!(%chain, %symbol, %wrapped_token, decimals, "Drain refused: the wrapped token is not 18 decimals");
            return Err(onchain_failure(
                ROUTE,
                chain,
                &format!("wrapped token {wrapped_token} has {decimals} decimals, not 18"),
            ));
        }

        let withdraw_tx = raindex
            .submit_withdraw(wrapped_token, RaindexVaultId(vault_id), amount, decimals)
            .await
            .map_err(|error| onchain_failure(ROUTE, chain, &error))?;
        info!(
            %chain, %symbol, %wrapped_token, %vault_id, %amount, %withdraw_tx,
            "Drain vault withdrawal broadcast via API"
        );

        let rest = async move {
            let drained = async {
                let withdrawn = confirm_vault_tx(
                    &raindex,
                    VaultTx {
                        route: ROUTE,
                        chain,
                        token: wrapped_token,
                        vault_id,
                        amount_raw: amount,
                        tx: withdraw_tx,
                        required_confirmations,
                    },
                    Some(&symbol),
                )
                .await;
                drop(withdraw_guard);
                if !withdrawn {
                    error!(
                        %chain, %symbol, %withdraw_tx,
                        "Drain stopped: its vault withdrawal did not confirm, so the shares are \
                         still in the vault"
                    );
                    return None;
                }

                let unwrap_tx = match wrapper
                    .submit_unwrap(wrapped_token, amount, owner, owner)
                    .await
                {
                    Ok(unwrap_tx) => unwrap_tx,
                    Err(error) => {
                        error!(
                            %chain, %symbol, %withdraw_tx, ?error,
                            "Drain stopped after its vault withdrawal: the unwrap was not sent, \
                             so the wrapped shares are in the bot's wallet. Check the chain for \
                             an unwrap before running capital unwrap-equity"
                        );
                        return None;
                    }
                };
                info!(%chain, %symbol, %amount, %unwrap_tx, "Drain unwrap broadcast via API");
                let pending = |tx| PendingEquityTx {
                    wallet: &wallet,
                    chain,
                    symbol: &symbol,
                    tx,
                    required_confirmations,
                };
                let underlying = match confirm_wrapper_tx(
                    &wrapper,
                    WrapperOperation::Unwrap,
                    wrapped_token,
                    amount,
                    pending(unwrap_tx),
                )
                .await
                {
                    WrapperFate::Received(underlying) => underlying,
                    WrapperFate::ConfirmedUnaccounted => {
                        error!(
                            %chain, %symbol, %unwrap_tx,
                            "Drain stopped after its unwrap: the unwrap confirmed but its payout \
                             cannot be read, so nothing was sent to Alpaca. Read the underlying \
                             balance onchain and run capital redeem for it"
                        );
                        return None;
                    }
                    WrapperFate::Failed => {
                        error!(
                            %chain, %symbol, %unwrap_tx,
                            "Drain stopped: its unwrap did not confirm, so the wrapped shares \
                             are in the bot's wallet"
                        );
                        return None;
                    }
                };

                // Sent exactly once: the transfer carries no idempotency key,
                // so a retry could send the shares twice (RAI-1494).
                let transfer_tx = match tokenizer.submit_redemption_send(token, underlying).await {
                    Ok(transfer_tx) => transfer_tx,
                    Err(error) => {
                        error!(
                            %chain, %symbol, %unwrap_tx, %underlying, ?error,
                            "Drain stopped after its unwrap: the redemption transfer failed to \
                             send, so the underlying is in the bot's wallet. Check the chain for \
                             a transfer before running capital redeem"
                        );
                        return None;
                    }
                };
                info!(
                    %chain, %symbol, %token, %redemption_wallet, %underlying, %transfer_tx,
                    "Drain redemption transfer broadcast via API"
                );
                confirm_redemption_transfer(
                    &tokenizer,
                    &wallet,
                    &symbol,
                    chain,
                    transfer_tx,
                    required_confirmations,
                )
                .await
                .then_some(transfer_tx)
            };
            if let Some(transfer_tx) =
                hold_until_proven(claim, ROUTE, &symbol, withdraw_tx, drained).await
            {
                follow_alpaca_redemption(&tokenizer, &symbol, chain, transfer_tx).await;
            }
        };
        Ok::<_, OpsError>((withdraw_tx, rest))
    })
    .await?;

    Ok(Json(DrainEquityResponse {
        chain,
        symbol: symbol.to_string(),
        quantity: quantity.inner().to_string(),
        vault_id,
        redemption_wallet,
        withdraw_tx,
    }))
}

/// The chain's configured Alpaca redemption wallet, never one a request
/// names: a wallet chosen by the caller would let anyone in the capital group
/// send the bot's shares out of our accounts.
fn configured_redemption_wallet(state: &AppState, chain: Chain) -> Result<Address, OpsError> {
    state
        .ctx
        .redemption_wallet(chain)
        .map_err(|error| unprocessable(format!("{error}; configure it for {chain}")))
}

/// The tokenization client a redemption sends and polls through, built from
/// the bot's broker ctx and signer as the conductor builds its own.
fn redemption_tokenizer(
    state: &AppState,
    chain: Chain,
    wallet: &Arc<dyn Wallet<Provider = RootProvider>>,
    redemption_wallet: Address,
) -> Result<RedemptionTokenizer, OpsError> {
    let BrokerCtx::AlpacaBrokerApi(alpaca) = &state.ctx.broker;
    AlpacaTokenizationService::new(
        alpaca.base_url().to_string(),
        alpaca.account_id,
        alpaca.auth.clone(),
        Arc::clone(wallet),
        chain,
        Some(redemption_wallet),
    )
    .map_err(|error| {
        error!(%chain, ?error, "Capital route refused: the Alpaca tokenization client failed to build");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "The bot could not build its Alpaca tokenization client".to_string(),
            }),
        )
    })
}

/// Awaits the redemption transfer's confirmation like the wrap routes, and
/// returns whether it moved the shares: `false` once it reverted, which moves
/// none.
async fn confirm_redemption_transfer(
    tokenizer: &RedemptionTokenizer,
    wallet: &Arc<dyn Wallet<Provider = RootProvider>>,
    symbol: &Symbol,
    chain: Chain,
    transfer_tx: TxHash,
    required_confirmations: u64,
) -> bool {
    let mut unproven = UnprovenCapitalTx::new(tokio::time::Instant::now());
    loop {
        let error = match tokenizer.confirm_redemption_send(transfer_tx).await {
            Ok(RedemptionSendReceipt::Succeeded) => {
                info!(%chain, %symbol, %transfer_tx, "Redemption transfer confirmed via API");
                return true;
            }
            Ok(RedemptionSendReceipt::Reverted { cause }) => {
                error!(
                    %chain, %symbol, %transfer_tx, cause,
                    "Redemption transfer sent via API reverted; no shares moved"
                );
                return false;
            }
            Err(error) => error,
        };
        let mined = mined_or_unknown(wallet, "redeem", chain, transfer_tx).await;
        match redemption_send_fate(&error, mined.as_ref(), required_confirmations) {
            TxFate::Confirmed => {
                info!(
                    %chain, %symbol, %transfer_tx, ?error,
                    "Redemption transfer confirmed via API"
                );
                return true;
            }
            TxFate::Failed => {
                error!(
                    %chain, %symbol, %transfer_tx, ?error,
                    "Redemption transfer sent via API did not confirm"
                );
                return false;
            }
            TxFate::Unknown => {
                warn!(
                    %chain, %symbol, %transfer_tx, ?error,
                    "Redemption transfer sent via API is not confirmed yet; keeping its lock \
                     and waiting again"
                );
                unproven.observe(
                    tokio::time::Instant::now(),
                    "redeem",
                    chain,
                    symbol,
                    transfer_tx,
                );
                tokio::time::sleep(CONFIRM_RETRY_DELAY).await;
            }
        }
    }
}

/// Polls Alpaca until it detects the confirmed redemption transfer and the
/// redemption reaches a terminal status, and logs both, as `st0x-cli
/// alpaca-redeem` printed them. The client keeps the bot's default
/// backpressure policy: a throttled lookup is waited out inside the poll, as
/// in the bot's own redemption jobs, where the CLI handed it back and retried
/// it with `retry_on_backpressure`.
async fn follow_alpaca_redemption(
    tokenizer: &RedemptionTokenizer,
    symbol: &Symbol,
    chain: Chain,
    transfer_tx: TxHash,
) {
    let detected = match tokenizer.poll_for_redemption(&transfer_tx).await {
        Ok(request) => request,
        Err(error) => {
            error!(
                %chain, %symbol, %transfer_tx, ?error,
                "Alpaca did not report a redemption of the transfer sent via API; check its \
                 tokenization requests before sending again"
            );
            return;
        }
    };
    info!(
        %chain, %symbol, %transfer_tx, request_id = %detected.id, status = ?detected.status,
        "Alpaca detected the redemption transfer sent via API"
    );

    let finished = if detected.status == TokenizationRequestStatus::Pending {
        match tokenizer.poll_redemption_until_complete(&detected.id).await {
            Ok(request) => request,
            Err(error) => {
                error!(
                    %chain, %symbol, %transfer_tx, request_id = %detected.id, ?error,
                    "Alpaca's redemption of the transfer sent via API did not finish; check its \
                     tokenization request"
                );
                return;
            }
        }
    } else {
        detected
    };

    match finished.status {
        TokenizationRequestStatus::Completed => info!(
            %chain, %symbol, %transfer_tx, request_id = %finished.id,
            "Alpaca completed the redemption sent via API"
        ),
        TokenizationRequestStatus::Rejected => error!(
            %chain, %symbol, %transfer_tx, request_id = %finished.id,
            "Alpaca rejected the redemption sent via API"
        ),
        TokenizationRequestStatus::Pending => warn!(
            %chain, %symbol, %transfer_tx, request_id = %finished.id,
            "Alpaca's redemption of the transfer sent via API is still pending after its poll"
        ),
    }
}

/// The route name `capital alpaca-deposit` answers and logs under.
const ALPACA_DEPOSIT_ROUTE: &str = "alpaca-deposit";

/// How long `alpaca-deposit` waits for Alpaca's deposit address before it
/// refuses with 502. The wallet client sets only a connect timeout, so this
/// bounds the lookup, well under the operator client's 30 second request
/// timeout.
#[cfg(not(test))]
const DEPOSIT_ADDRESS_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

#[cfg(test)]
const DEPOSIT_ADDRESS_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(500);

/// Sends `amount` USDC from the bot's Ethereum wallet to the deposit address
/// Alpaca gives for the bot's own account, which Alpaca credits to the
/// account. Mirrors `st0x-cli alpaca-deposit`, signed by the bot's own
/// wallet; the bot's deposit leg sends the same transfer.
///
/// The request names no chain and no destination: the send is always the
/// corridor's Ethereum USDC, the token the deposit leg sends, to the address
/// [`alpaca_usdc_deposit_address`] returns, the lookup the deposit leg makes.
/// A destination chosen by the caller would let anyone in the capital group
/// send the bot's USDC out of our accounts.
///
/// The request carries the client's operation id, and each id holds at most
/// one transfer (`AlpacaDepositOperation`, the design of ADR 0023). The route
/// signs the transfer, records the signed bytes under the id, then broadcasts
/// them, so a rerun with the same id, even after a dropped request or a
/// restart, reports the recorded transfer or sends the same bytes again, and
/// never signs a second one. A known id whose transfer is settled, or pending
/// and already held by a node, is answered at once, since it sends nothing; a
/// pending transfer no node holds is sent again under the lock and the pause,
/// like a new one.
///
/// Input validation and the startup gate come first, as on every capital
/// route, then Alpaca's deposit address (502), asked before anything is held
/// and bounded by [`DEPOSIT_ADDRESS_TIMEOUT`], so a hung Alpaca blocks no USDC
/// transfer and no verb that takes the resume lock. Everything after runs
/// detached, in order: the resume lock (409), the USDC driver pause (503), no
/// USDC transfer in flight or holding a corridor guard on any corridor (409,
/// [`RebalancingService::ensure_no_usdc_transfer_in_flight`]), since every
/// transfer may need the Ethereum wallet's USDC, the Ethereum wallet's gas
/// (503), the wallet's USDC balance (409), then the signed transfer, its
/// record and its broadcast. The lock and the pause are held like
/// `cctp-bridge` holds them, from before the balance read until the
/// transfer's fate is proven, so no transfer spends the balance in between
/// and the bot's own deposit sends never race this one for a nonce.
///
/// The route answers at the broadcast. A broadcast that fails answers 502
/// while the chain does not prove the transfer's fate, but keeps the lock and
/// the pause and follows the recorded hash all the same: a node may have
/// accepted the bytes before the answer was lost. The detached task then
/// confirms the transfer ([`confirm_recorded_deposit`]): it sends the same
/// bytes again while the fate is unknown, records `confirmed` or `reverted`
/// from the transfer's own receipt, or `superseded` once the wallet's
/// confirmed nonce passed the transfer's with no receipt for it, and alerts
/// once while it stays unproven ([`UnprovenCapitalTx`]). Only then are the
/// lock and the pause freed, and a transfer that moved the USDC is followed
/// with `poll_deposit_by_tx_hash` until Alpaca reports its deposit final; the
/// transfer id and the final status are logged, and a deposit Alpaca does not
/// complete alerts. A restart restores a pending transfer and holds both
/// again until it is proven ([`restore_capital_alpaca_deposits`]).
pub(super) async fn alpaca_deposit(
    State(state): State<AppState>,
    Audited(request): Audited<AlpacaDepositRequest>,
) -> Result<Json<AlpacaDepositResponse>, OpsError> {
    let chain = Chain::Ethereum;
    let (amount, amount_raw) = positive_usdc(&request.amount)?;
    require_startup_complete(&state, ALPACA_DEPOSIT_ROUTE)?;
    let handle = state.recovery.get().ok_or_else(recovery_not_ready)?;
    let wallet = Arc::clone(bot_wallets(&state)?.ethereum_wallet());
    let usdc = state.ctx.rebalancing.cctp_corridor.usdc_ethereum();
    let required_confirmations =
        deposit_send_required_confirmations(&state.ctx.chains).map_err(|error| {
            error!(%error, "Alpaca deposit refused: Ethereum has no confirmation depth");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: error.to_string(),
                }),
            )
        })?;
    let store = Arc::clone(&handle.alpaca_deposit_store);
    let request = DepositRequest {
        operation_id: request.operation_id,
        amount: amount.inner().to_string(),
        amount_raw,
        required_confirmations,
    };

    // A settled deposit, or a pending one a node already holds, sends nothing
    // new, so it is answered at once, without Alpaca, the lock and the pause.
    // A pending one no node holds must be sent again, which spends the
    // wallet's USDC, so it takes the locked path below.
    if let Some(recorded) = load_deposit_operation(&store, &request.operation_id).await? {
        ensure_same_deposit(&request, &recorded)?;
        if recorded.outcome != AlpacaDepositOutcome::Pending
            || deposit_known_to_node(&wallet, &recorded.transfer).await
        {
            return Ok(Json(
                report_recorded_deposit(&store, &wallet, &request, &recorded).await,
            ));
        }
    }

    let alpaca_wallet = Arc::new(alpaca_wallet_service(&state.ctx)?);
    let _early = Arc::clone(&state.resume_lock.0).try_lock_owned();
    let deposit_address = lookup_deposit_address(&alpaca_wallet, &request).await?;
    // Checked under the lock and the pause, with the balance read.
    let gas = signer_gas_readiness(&state, chain, &wallet);
    let rebalancing = Arc::clone(&handle.rebalancing_service);
    let resume_lock = Arc::clone(&state.resume_lock.0);
    let driver_pause = Arc::clone(&handle.usdc_driver_pause);
    let pool = state.pool.clone();

    answer_from_detached(
        &state.detached_tasks,
        ALPACA_DEPOSIT_ROUTE,
        request.operation_id,
        async move {
            // The lock and the pause stay held until the transfer's fate is
            // proven; the confirmation future owns them.
            let held = (
                resume_lock.try_lock_owned().map_err(|_| {
                    conflict("A resume or recheck operation is already in progress".to_string())
                })?,
                quiesce_usdc_driver(&driver_pause, UsdcDriverPauseRequest::AlpacaDeposit).await?,
            );

            // A request with the same id may have recorded its transfer since
            // the lookup above; the lock serializes deposits, so this read is
            // final.
            if let Some(recorded) = load_deposit_operation(&store, &request.operation_id).await? {
                return answer_recorded_deposit_under_lock(
                    store,
                    wallet,
                    alpaca_wallet,
                    request,
                    &recorded,
                    held,
                )
                .await;
            }

            rebalancing
                .ensure_no_usdc_transfer_in_flight(&pool)
                .await
                .map_err(|error| {
                    warn!(?error, amount = %request.amount, "Alpaca deposit refused: a USDC transfer may need the Ethereum wallet's USDC");
                    let (status, message) = usdc_resume_error_response(&error);
                    (status, Json(ErrorResponse { error: message }))
                })?;
            ensure_signer_gas(&*gas?, ALPACA_DEPOSIT_ROUTE, chain).await?;

            let sender = wallet.address();
            let balance = wallet
                .call::<OpenChainErrorRegistry, _>(usdc, IERC20::balanceOfCall { account: sender })
                .await
                .map_err(|error| onchain_failure(ALPACA_DEPOSIT_ROUTE, chain, &error))?;
            if balance < amount_raw {
                warn!(%sender, %balance, %amount_raw, "Alpaca deposit refused: insufficient USDC balance");
                return Err(conflict(format!(
                    "Insufficient USDC balance in the bot's Ethereum wallet {sender}: it holds \
                     {balance} and the deposit needs {amount_raw} (USDC base units, 6 decimals); \
                     nothing was sent"
                )));
            }

            // Signing broadcasts nothing; it reserves the transfer's nonce.
            let prepared = wallet
                .prepare_pending(
                    usdc,
                    Bytes::from(
                        IERC20::transferCall {
                            to: deposit_address,
                            amount: amount_raw,
                        }
                        .abi_encode(),
                    ),
                    DEPOSIT_TRANSFER_NOTE,
                )
                .await
                .map_err(|error| onchain_failure(ALPACA_DEPOSIT_ROUTE, chain, &error))?;
            let transfer = DepositTransfer {
                amount: amount_raw,
                deposit_address,
                prepared,
            };
            let persisted = store
                .send(
                    &request.operation_id,
                    AlpacaDepositOperationCommand::Prepare {
                        transfer: transfer.clone(),
                    },
                )
                .await;
            if let Err(error) = persisted {
                error!(operation_id = %request.operation_id, transfer_tx = %transfer.transfer_tx(), ?error, "Failed to record the signed Alpaca deposit transfer; not broadcasting it");
                let recorded =
                    release_unpersisted_deposit(&wallet, &store, &request, &transfer).await?;
                if recorded.transfer.transfer_tx() != transfer.transfer_tx() {
                    return answer_recorded_deposit_under_lock(
                        store,
                        wallet,
                        alpaca_wallet,
                        request,
                        &recorded,
                        held,
                    )
                    .await;
                }
            }

            broadcast_deposit_and_answer(store, wallet, alpaca_wallet, request, transfer, held)
                .await
        },
    )
    .await?
    .map(Json)
}

/// What one `alpaca-deposit` request asked for, as the record, report and
/// confirm paths need it.
#[derive(Clone)]
struct DepositRequest {
    operation_id: AlpacaDepositOperationId,
    /// The amount as a decimal USDC string, as the answer reports it.
    amount: String,
    /// The amount in USDC base units (6 decimals).
    amount_raw: U256,
    required_confirmations: u64,
}

/// What `alpaca-deposit` answers once its transfer is signed: the answer
/// itself, a 502 for a failed broadcast whose transfer is still followed,
/// and the future that follows the transfer until its fate is proven.
type DepositAnswer = (
    Result<AlpacaDepositResponse, OpsError>,
    BoxFuture<'static, ()>,
);

async fn load_deposit_operation(
    store: &Store<AlpacaDepositOperation>,
    id: &AlpacaDepositOperationId,
) -> Result<Option<AlpacaDepositOperation>, OpsError> {
    store.load(id).await.map_err(|error| {
        error!(operation_id = %id, ?error, "Could not load the Alpaca deposit operation");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!(
                    "Could not read operation {id}; nothing was sent by this request, retry \
                     with the same operation id"
                ),
            }),
        )
    })
}

/// Refuses a request whose amount differs from the transfer its operation id
/// holds: the id names one deposit.
fn ensure_same_deposit(
    request: &DepositRequest,
    recorded: &AlpacaDepositOperation,
) -> Result<(), OpsError> {
    if recorded.transfer.amount == request.amount_raw {
        return Ok(());
    }
    let operation_id = request.operation_id;
    let transfer_tx = recorded.transfer.transfer_tx();
    warn!(
        %operation_id, %transfer_tx, recorded_amount_raw = %recorded.transfer.amount,
        amount_raw = %request.amount_raw,
        "Alpaca deposit refused: the operation id already holds a different deposit"
    );
    Err(conflict(format!(
        "Operation id {operation_id} already holds deposit {transfer_tx} of {} USDC base units; \
         use a new operation id for a different deposit",
        recorded.transfer.amount
    )))
}

/// Answers with the transfer an operation id holds, sending nothing: a
/// settled one reports its outcome, and a pending one reports what the chain
/// now proves.
async fn report_recorded_deposit(
    store: &Store<AlpacaDepositOperation>,
    wallet: &DepositWallet,
    request: &DepositRequest,
    recorded: &AlpacaDepositOperation,
) -> AlpacaDepositResponse {
    let status = deposit_status(
        store,
        wallet,
        &request.operation_id,
        recorded,
        request.required_confirmations,
    )
    .await;
    info!(operation_id = %request.operation_id, transfer_tx = %recorded.transfer.transfer_tx(), ?status, "Alpaca deposit reported via API: the operation id already holds this transfer");
    AlpacaDepositResponse::of(request, &recorded.transfer, status)
}

/// Whether a node already holds the pending transfer, mined or in its
/// mempool, so a rerun has nothing to send. A failed read counts as unknown:
/// the rerun then sends the same bytes under the lock and the pause.
async fn deposit_known_to_node(wallet: &DepositWallet, transfer: &DepositTransfer) -> bool {
    matches!(
        wallet
            .provider()
            .get_transaction_by_hash(transfer.transfer_tx())
            .await,
        Ok(Some(_))
    )
}

/// Asks Alpaca for the account's USDC deposit address on Ethereum, bounded
/// by [`DEPOSIT_ADDRESS_TIMEOUT`]. Asked before the resume lock and the USDC
/// driver pause are taken, so a slow or hung Alpaca answers 502 without ever
/// holding either.
async fn lookup_deposit_address(
    alpaca_wallet: &AlpacaWalletService,
    request: &DepositRequest,
) -> Result<Address, OpsError> {
    let refused = |reason: String| {
        warn!(operation_id = %request.operation_id, amount = %request.amount, %reason, "Alpaca deposit refused: Alpaca gave no USDC deposit address");
        (
            StatusCode::BAD_GATEWAY,
            Json(ErrorResponse {
                error: format!(
                    "Alpaca did not give the account's USDC deposit address on Ethereum: \
                     {reason}; nothing was sent"
                ),
            }),
        )
    };
    match tokio::time::timeout(
        DEPOSIT_ADDRESS_TIMEOUT,
        alpaca_usdc_deposit_address(alpaca_wallet),
    )
    .await
    {
        Ok(Ok(address)) => Ok(address),
        Ok(Err(error)) => Err(refused(error.to_string())),
        Err(_elapsed) => Err(refused(format!(
            "no answer within {} seconds",
            DEPOSIT_ADDRESS_TIMEOUT.as_secs_f64()
        ))),
    }
}

/// Answers, under the resume lock and the driver pause, a request whose
/// operation id already holds a transfer: a settled one is reported, and a
/// pending one is sent again from its recorded bytes, like a new deposit,
/// since sending spends the wallet's USDC.
async fn answer_recorded_deposit_under_lock<Held: Send + 'static>(
    store: Arc<Store<AlpacaDepositOperation>>,
    wallet: DepositWallet,
    alpaca_wallet: Arc<AlpacaWalletService>,
    request: DepositRequest,
    recorded: &AlpacaDepositOperation,
    held: Held,
) -> Result<DepositAnswer, OpsError> {
    ensure_same_deposit(&request, recorded)?;
    if recorded.outcome != AlpacaDepositOutcome::Pending {
        let response = report_recorded_deposit(&store, &wallet, &request, recorded).await;
        return Ok((Ok(response), std::future::ready(()).boxed()));
    }
    broadcast_deposit_and_answer(
        store,
        wallet,
        alpaca_wallet,
        request,
        recorded.transfer.clone(),
        held,
    )
    .await
}

/// Broadcasts a recorded transfer's signed bytes while the caller holds the
/// resume lock and the driver pause in `held`, and answers with the transfer
/// as pending; the returned future confirms it and holds `held` until its
/// fate is proven. A failed broadcast proves nothing: the node may have
/// accepted the bytes before its answer was lost. So the transfer is
/// followed and `held` kept all the same, and the request answers what the
/// chain already proves, or `502` while it proves nothing. A transfer signed
/// by another address than the wallet (a rotated key) is never broadcast
/// through this wallet, whose nonce bookkeeping is per address; it spends
/// another wallet's USDC, so nothing is held for it.
async fn broadcast_deposit_and_answer<Held: Send + 'static>(
    store: Arc<Store<AlpacaDepositOperation>>,
    wallet: DepositWallet,
    alpaca_wallet: Arc<AlpacaWalletService>,
    request: DepositRequest,
    transfer: DepositTransfer,
    held: Held,
) -> Result<DepositAnswer, OpsError> {
    let operation_id = request.operation_id;
    let transfer_tx = transfer.transfer_tx();
    if !transfer.prepared.signed_by(wallet.address()) {
        warn!(%operation_id, %transfer_tx, signer = ?transfer.prepared.signer(), wallet = %wallet.address(), "Alpaca deposit not sent: the recorded transfer was signed by another wallet");
        return Err((
            StatusCode::BAD_GATEWAY,
            Json(ErrorResponse {
                error: format!(
                    "Deposit {transfer_tx} is recorded under operation id {operation_id}, but it \
                     was signed by {:?}, not this bot's wallet {} (was the key rotated?), so \
                     this bot never broadcasts it; check it onchain",
                    transfer.prepared.signer(),
                    wallet.address()
                ),
            }),
        ));
    }

    let answer = match wallet
        .broadcast_prepared(&transfer.prepared, DEPOSIT_TRANSFER_NOTE)
        .await
    {
        Ok(_) => {
            info!(
                %operation_id, sender = %wallet.address(), deposit_address = %transfer.deposit_address,
                amount = %request.amount, amount_raw = %transfer.amount, %transfer_tx,
                "Alpaca deposit transfer broadcast via API"
            );
            Ok(AlpacaDepositResponse::of(
                &request,
                &transfer,
                AlpacaDepositStatus::Pending,
            ))
        }
        Err(error) => {
            // The error's text can carry the RPC URL, whose path or query
            // holds the provider key, so it is logged scrubbed and never put
            // in the answer.
            error!(
                %operation_id, %transfer_tx, error = %scrubbed(&error),
                "Broadcasting the recorded Alpaca deposit transfer failed; following it until \
                 its fate is proven"
            );
            let status = settle_pending_deposit(
                &store,
                &wallet,
                &operation_id,
                &transfer,
                request.required_confirmations,
            )
            .await;
            if status != AlpacaDepositStatus::Pending {
                let response = AlpacaDepositResponse::of(&request, &transfer, status);
                let after = after_proven_deposit(alpaca_wallet, transfer, status);
                return Ok((Ok(response), after.boxed()));
            }
            Err((
                StatusCode::BAD_GATEWAY,
                Json(ErrorResponse {
                    error: format!(
                        "Deposit {transfer_tx} is recorded under operation id {operation_id}, \
                         but its broadcast failed (see the bot logs). A node may still hold \
                         it, so the bot keeps the resume lock and the USDC driver pause, sends \
                         the same bytes again until its fate is proven, then frees both; rerun \
                         with the same operation id to see its status"
                    ),
                }),
            ))
        }
    };

    let follow = async move {
        let status = hold_until_deposits_proven(
            held,
            &[transfer_tx],
            confirm_recorded_deposit(
                &store,
                &wallet,
                &operation_id,
                &transfer,
                request.required_confirmations,
            ),
        )
        .await;
        after_proven_deposit(alpaca_wallet, transfer, status).await;
    };
    Ok((answer, follow.boxed()))
}

/// What follows a deposit whose fate is proven: one that moved the USDC is
/// followed on Alpaca until its deposit is final.
async fn after_proven_deposit(
    alpaca_wallet: Arc<AlpacaWalletService>,
    transfer: DepositTransfer,
    status: AlpacaDepositStatus,
) {
    if status == AlpacaDepositStatus::Confirmed {
        follow_alpaca_deposit(
            &alpaca_wallet,
            transfer.deposit_address,
            transfer.transfer_tx(),
        )
        .await;
    }
}

/// Runs `wait`, which returns once the fate of every transfer in
/// `transfer_txs` is proven, and frees `held` (the resume lock and the USDC
/// driver pause) after it. A panic would unwind through `held` and free both
/// while a fate is unknown, so it is caught here, `held` is kept until a
/// restart (which restores the transfers and holds both again), and the
/// panic resumes for its task's join failure log.
async fn hold_until_deposits_proven<Held, Proven>(
    held: Held,
    transfer_txs: &[TxHash],
    wait: impl Future<Output = Proven>,
) -> Proven {
    match std::panic::AssertUnwindSafe(wait).catch_unwind().await {
        Ok(proven) => {
            drop(held);
            proven
        }
        Err(panic) => {
            error!(
                ?transfer_txs,
                "Alpaca deposit confirmation panicked; keeping the resume lock and the USDC \
                 driver pause until a restart, which restores the transfer and follows it again"
            );
            std::mem::forget(held);
            std::panic::resume_unwind(panic);
        }
    }
}

/// After a failed `Prepare` write, reloads the operation. These bytes
/// recorded (the write committed): returns them, and the route broadcasts.
/// Another transfer recorded under the id: releases this signature's nonce
/// and returns the recorded transfer to adopt. Nothing recorded: releases the
/// nonce, since nothing will broadcast it. A failed reload cannot tell, so
/// the nonce stays reserved until a restart: a rerun before then could sign
/// behind it. Mirrors `release_unpersisted_burn`.
async fn release_unpersisted_deposit(
    wallet: &DepositWallet,
    store: &Store<AlpacaDepositOperation>,
    request: &DepositRequest,
    transfer: &DepositTransfer,
) -> Result<AlpacaDepositOperation, OpsError> {
    let operation_id = request.operation_id;
    let transfer_tx = transfer.transfer_tx();
    let nonce = transfer.prepared.nonce();
    match store.load(&operation_id).await {
        Ok(Some(recorded)) if recorded.transfer.transfer_tx() == transfer_tx => {
            warn!(%operation_id, %transfer_tx, "The failed Alpaca deposit write committed; broadcasting it");
            Ok(recorded)
        }
        Ok(Some(recorded)) => {
            warn!(%operation_id, %transfer_tx, nonce, recorded = %recorded.transfer.transfer_tx(), "Releasing the nonce of a signed Alpaca deposit transfer: the operation id already holds another transfer");
            wallet.discard_prepared(transfer_tx).await;
            Ok(recorded)
        }
        Ok(None) => {
            warn!(%operation_id, %transfer_tx, nonce, "Releasing the nonce of a signed Alpaca deposit transfer that was not recorded");
            wallet.discard_prepared(transfer_tx).await;
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!(
                        "Could not record the signed deposit under operation id \
                         {operation_id}; nothing was broadcast, so a rerun with the same \
                         operation id deposits once"
                    ),
                }),
            ))
        }
        Err(error) => {
            error!(target: "operational_alert", alert = true, kind = AlertKind::CapitalAlpacaDepositRecordUnknown.as_str(), %operation_id, %transfer_tx, nonce, ?error, "Cannot tell whether a signed capital Alpaca deposit was recorded; its nonce stays reserved and every later send from the Ethereum wallet waits behind it until a restart");
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!(
                        "Could not tell whether deposit {transfer_tx} was recorded under \
                         operation id {operation_id}; nothing was broadcast, and its nonce \
                         stays reserved until the bot restarts. Rerun with the same operation \
                         id only after a restart: it then broadcasts the deposit if it was \
                         recorded, or deposits once if not"
                    ),
                }),
            ))
        }
    }
}

/// The Alpaca wallet client a deposit looks its address up and polls
/// through, built from the bot's broker ctx as the conductor builds its own.
/// It keeps the default backpressure policy: a throttled deposit poll is
/// waited out inside the poll on the detached task, where the transfer job's
/// client hands it back to reschedule its worker.
fn alpaca_wallet_service(ctx: &Ctx) -> Result<AlpacaWalletService, OpsError> {
    let BrokerCtx::AlpacaBrokerApi(alpaca) = &ctx.broker;
    AlpacaWalletService::new(
        alpaca.base_url().to_string(),
        alpaca.account_id,
        alpaca.auth.clone(),
    )
    .map_err(|error| {
        error!(
            ?error,
            "Capital route refused: the Alpaca wallet client failed to build"
        );
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "The bot could not build its Alpaca wallet client".to_string(),
            }),
        )
    })
}

/// Follows a recorded deposit transfer until the chain proves its fate, and
/// returns it, never `Pending`. The wallet's receipt wait only paces the
/// loop: it keeps the first receipt it saw while it counts blocks, so the
/// outcome is recorded from a fresh read ([`settle_pending_deposit`]): the
/// transfer's own receipt at the required confirmations, or its signer's
/// confirmed nonce past the transfer's with no receipt for it, which settles
/// it `superseded` and releases its nonce. While nothing is proven the
/// transfer may still land, and freeing the lock and the pause would let a
/// USDC transfer spend the balance it may still take, so the loop sends the
/// same bytes again (a node may have dropped them from its mempool) and waits
/// again. A fate that stays unknown alerts once ([`UnprovenCapitalTx`]).
async fn confirm_recorded_deposit(
    store: &Store<AlpacaDepositOperation>,
    wallet: &DepositWallet,
    operation_id: &AlpacaDepositOperationId,
    transfer: &DepositTransfer,
    required_confirmations: u64,
) -> AlpacaDepositStatus {
    let transfer_tx = transfer.transfer_tx();
    let deposit_address = transfer.deposit_address;
    let amount_raw = transfer.amount;
    let mut unproven = UnprovenCapitalTx::new(tokio::time::Instant::now());
    loop {
        let waited = wallet
            .confirm::<OpenChainErrorRegistry>(transfer_tx)
            .await
            .err()
            .map(|error| scrubbed(&error));
        match settle_pending_deposit(
            store,
            wallet,
            operation_id,
            transfer,
            required_confirmations,
        )
        .await
        {
            AlpacaDepositStatus::Pending => {}
            AlpacaDepositStatus::Confirmed => {
                info!(%operation_id, %deposit_address, %amount_raw, %transfer_tx, ?waited, "Alpaca deposit transfer confirmed via API");
                return AlpacaDepositStatus::Confirmed;
            }
            AlpacaDepositStatus::Reverted => {
                error!(%operation_id, %deposit_address, %amount_raw, %transfer_tx, ?waited, "Alpaca deposit transfer sent via API reverted; no USDC moved, and a new operation id deposits again");
                return AlpacaDepositStatus::Reverted;
            }
            AlpacaDepositStatus::Superseded => {
                warn!(%operation_id, %deposit_address, %amount_raw, %transfer_tx, "Alpaca deposit transfer sent via API can never mine: another tx took its nonce; freeing the resume lock and the USDC driver pause");
                return AlpacaDepositStatus::Superseded;
            }
        }
        warn!(
            %operation_id, %deposit_address, %amount_raw, %transfer_tx, ?waited,
            "Alpaca deposit transfer sent via API is not confirmed yet; keeping the resume lock \
             and the USDC driver pause, sending the same bytes again and waiting again"
        );
        unproven.observe_alpaca_deposit(tokio::time::Instant::now(), deposit_address, transfer_tx);
        if let Err(error) = wallet
            .broadcast_prepared(&transfer.prepared, DEPOSIT_TRANSFER_NOTE)
            .await
        {
            warn!(%operation_id, %transfer_tx, error = %scrubbed(&error), "Sending the Alpaca deposit transfer's bytes again failed; retrying on the next round");
        }
        tokio::time::sleep(CONFIRM_RETRY_DELAY).await;
    }
}

/// What the startup restore of capital Alpaca deposits leaves to the
/// conductor.
pub(crate) struct RestoredCapitalAlpacaDeposits {
    /// Startup skips the Ethereum approvals and revokes: a restored transfer
    /// is not mined, or the pending ones could not all be read.
    pub(crate) gates_ethereum: bool,
    /// The task that holds the resume lock and the USDC driver pause until
    /// every restored transfer is proven, for the conductor to spawn.
    pub(crate) follow: Option<BoxFuture<'static, ()>>,
}

/// At startup, restores every pending `alpaca-deposit` transfer
/// ([`restore_pending_alpaca_deposits`]: its nonce reserved again and its
/// bytes rebroadcast) and, while any is unproven, holds again what the live
/// route held for it: the resume lock and the USDC driver pause, taken in the
/// route's order before any USDC worker runs. So no USDC transfer sizes
/// against USDC the transfer may still take, and no verb that takes the lock
/// sends from the wallet meanwhile, exactly as before the restart. The
/// returned task confirms each transfer like the route does
/// ([`confirm_recorded_deposit`]), frees both once all are proven, then
/// follows on Alpaca each that moved the USDC. Pending transfers that could
/// not be listed or loaded may still take USDC, so then both stay held until
/// a restart.
pub(crate) async fn restore_capital_alpaca_deposits(
    ctx: &Ctx,
    pool: &SqlitePool,
    store: &Arc<Store<AlpacaDepositOperation>>,
    resume_lock: &ResumeLock,
    driver_pause: &UsdcDriverPause,
) -> RestoredCapitalAlpacaDeposits {
    let wallet = match ctx.wallet() {
        Ok(wallets) => Arc::clone(wallets.ethereum_wallet()),
        Err(error) => {
            warn!(%error, "No signing wallet, so no capital Alpaca deposit can be pending; skipping their restore");
            return RestoredCapitalAlpacaDeposits {
                gates_ethereum: false,
                follow: None,
            };
        }
    };
    let required_confirmations = deposit_send_required_confirmations(&ctx.chains).ok();
    let RestoredAlpacaDeposits {
        restored,
        settled,
        pending,
        unreadable,
        unmined,
    } = restore_pending_alpaca_deposits(pool, store, &wallet, required_confirmations).await;
    info!(
        restored,
        settled,
        pending = pending.len(),
        unreadable,
        "Reserved the nonces of the pending capital Alpaca deposits at startup"
    );
    let gates_ethereum = unmined || unreadable;
    if pending.is_empty() && !unreadable {
        return RestoredCapitalAlpacaDeposits {
            gates_ethereum,
            follow: None,
        };
    }

    let lock = Arc::clone(&resume_lock.0).lock_owned().await;
    let pause = driver_pause.pause().await.inspect_err(|error| {
        error!(%error, "The USDC driver did not quiesce for a restored capital Alpaca deposit; USDC transfers may run before its fate is proven");
    });
    let held = (lock, pause);
    let required_confirmations = match required_confirmations {
        Some(required) if !unreadable => required,
        _ => {
            error!(
                unreadable,
                "Pending capital Alpaca deposits cannot all be read, or proven without Ethereum's \
                 required_confirmations; keeping the resume lock and the USDC driver pause until \
                 a restart"
            );
            std::mem::forget(held);
            return RestoredCapitalAlpacaDeposits {
                gates_ethereum: true,
                follow: None,
            };
        }
    };
    let alpaca_wallet = alpaca_wallet_service(ctx).ok().map(Arc::new);
    let store = Arc::clone(store);
    let follow = async move {
        let transfer_txs: Vec<TxHash> = pending
            .iter()
            .map(|(_, transfer)| transfer.transfer_tx())
            .collect();
        let wait = async {
            let mut proven = Vec::with_capacity(pending.len());
            for (operation_id, transfer) in pending {
                let status = confirm_recorded_deposit(
                    &store,
                    &wallet,
                    &operation_id,
                    &transfer,
                    required_confirmations,
                )
                .await;
                proven.push((status, transfer));
            }
            proven
        };
        let proven = hold_until_deposits_proven(held, &transfer_txs, wait).await;
        let Some(alpaca_wallet) = alpaca_wallet else {
            return;
        };
        for (status, transfer) in proven {
            after_proven_deposit(Arc::clone(&alpaca_wallet), transfer, status).await;
        }
    };
    RestoredCapitalAlpacaDeposits {
        gates_ethereum,
        follow: Some(follow.boxed()),
    }
}

/// Polls Alpaca with `poll_deposit_by_tx_hash` until it reports the
/// confirmed transfer's deposit final, and logs the transfer id and the
/// final status, as `st0x-cli alpaca-deposit` printed them. A deposit Alpaca
/// does not report, fails, or leaves in another status alerts: the USDC left
/// the wallet, and the account may not hold it.
async fn follow_alpaca_deposit(
    alpaca_wallet: &AlpacaWalletService,
    deposit_address: Address,
    transfer_tx: TxHash,
) {
    let transfer = match alpaca_wallet.poll_deposit_by_tx_hash(&transfer_tx).await {
        Ok(transfer) => transfer,
        Err(error) => {
            error!(
                target: "operational_alert",
                alert = true,
                kind = AlertKind::CapitalAlpacaDepositNotCredited.as_str(),
                %deposit_address, %transfer_tx, ?error,
                "Alpaca did not credit an operator deposit transfer: it did not report the \
                 transfer sent via API as a final deposit; check the account's transfers before \
                 sending again"
            );
            return;
        }
    };

    match transfer.status {
        TransferStatus::Complete => info!(
            %deposit_address, %transfer_tx, transfer_id = %transfer.id, amount = %transfer.amount,
            "Alpaca completed the deposit sent via API"
        ),
        TransferStatus::Failed => error!(
            target: "operational_alert",
            alert = true,
            kind = AlertKind::CapitalAlpacaDepositNotCredited.as_str(),
            %deposit_address, %transfer_tx, transfer_id = %transfer.id,
            "Alpaca did not credit an operator deposit transfer: Alpaca reports the deposit sent \
             via API as failed; check the transfer with Alpaca"
        ),
        status @ (TransferStatus::Pending | TransferStatus::Processing) => error!(
            target: "operational_alert",
            alert = true,
            kind = AlertKind::CapitalAlpacaDepositNotCredited.as_str(),
            %deposit_address, %transfer_tx, transfer_id = %transfer.id, ?status,
            "Alpaca did not credit an operator deposit transfer: the deposit sent via API ended \
             its poll in an unexpected status; check the transfer with Alpaca"
        ),
    }
}

/// Parses a decimal amount and refuses a zero or negative one, both as a 400.
/// `wrap` lifts the parsed value into the amount type the route works in.
fn positive_amount<Amount: HasZero + Into<Float>>(
    amount: &str,
    wrap: impl FnOnce(Float) -> Amount,
) -> Result<Positive<Amount>, (StatusCode, Json<ErrorResponse>)> {
    let parsed = Float::parse(amount.to_string())
        .map_err(|error| ops_precondition_error(format!("invalid amount {amount:?}: {error}")))?;
    Positive::new(wrap(parsed))
        .map_err(|_| ops_precondition_error(format!("amount must be positive, got {amount}")))
}

/// Parses a decimal USDC amount like [`positive_amount`] and converts it to
/// USDC base units (6 decimals), refusing as a 400 an amount the token cannot
/// hold: more than six decimals, or too large for a `U256`.
fn positive_usdc(amount: &str) -> Result<(Positive<Usdc>, U256), OpsError> {
    let amount = positive_amount(amount, Usdc::new)?;
    let raw = amount.inner().to_u256_6_decimals().map_err(|error| {
        ops_precondition_error(format!("invalid USDC amount {amount}: {error}"))
    })?;
    Ok((amount, raw))
}

/// Parses a decimal share quantity and refuses, as a 400, one that does not
/// parse or is not positive.
fn positive_quantity(quantity: &str) -> Result<Positive<FractionalShares>, OpsError> {
    let parsed = Float::parse(quantity.to_string()).map_err(|error| {
        ops_precondition_error(format!("invalid quantity {quantity:?}: {error}"))
    })?;
    Positive::new(FractionalShares::new(parsed))
        .map_err(|_| ops_precondition_error(format!("quantity must be positive, got {quantity}")))
}

/// Parses a decimal share quantity like [`positive_quantity`] and refuses, as
/// a 400, one finer than the nine decimals the tokenization API takes (the
/// precision the rebalancer truncates its own quantities to), so the worker
/// never receives a quantity Alpaca would reject.
fn positive_shares(quantity: &str) -> Result<Positive<FractionalShares>, OpsError> {
    let shares = positive_quantity(quantity)?;
    let parsed = shares.inner().inner();
    let (_, lossless) = parsed
        .to_fixed_decimal_lossy(ALPACA_QUANTITY_MAX_DECIMAL_PLACES)
        .map_err(|error| ops_precondition_error(format!("invalid quantity {quantity}: {error}")))?;
    if !lossless {
        return Err(ops_precondition_error(format!(
            "quantity {quantity} has more than {ALPACA_QUANTITY_MAX_DECIMAL_PLACES} decimals"
        )));
    }
    Ok(shares)
}

/// Parses the symbol and the share quantity of a wrap, unwrap or redeem, as
/// a 400 when either is malformed, and converts the quantity to the token's
/// smallest unit at the 18 decimals of every tokenized equity and wrapper.
/// The CLI's `to_u256_18_decimals` truncates a finer quantity; the route
/// refuses it instead, so it never moves less than it was asked to.
fn equity_wallet_input(
    symbol: &str,
    quantity: &str,
) -> Result<(Symbol, Positive<FractionalShares>, U256), OpsError> {
    let parsed_symbol = Symbol::new(symbol)
        .map_err(|error| ops_precondition_error(format!("invalid symbol {symbol:?}: {error}")))?;
    let shares = positive_quantity(quantity)?;
    let (amount, lossless) = shares
        .inner()
        .inner()
        .to_fixed_decimal_lossy(TOKENIZED_EQUITY_DECIMALS)
        .map_err(|error| ops_precondition_error(format!("invalid quantity {quantity}: {error}")))?;
    if !lossless {
        return Err(ops_precondition_error(format!(
            "quantity {quantity} has more than {TOKENIZED_EQUITY_DECIMALS} decimals"
        )));
    }
    Ok((parsed_symbol, shares, amount))
}

/// Resolves the burn amount from exactly one of `amount` and `all`; both or
/// neither is a 400, like the CLI's clap conflict.
fn burn_amount(
    amount: Option<&str>,
    all: bool,
) -> Result<RequestedBurn, (StatusCode, Json<ErrorResponse>)> {
    match (amount, all) {
        (Some(amount), false) => {
            let (_, raw) = positive_usdc(amount)?;
            Ok(RequestedBurn::Exact { amount: raw })
        }
        (None, true) => Ok(RequestedBurn::All),
        (Some(_), true) | (None, false) => Err(ops_precondition_error(
            "specify exactly one of amount and all",
        )),
    }
}

/// The same 503 the other recovery routes return before the conductor
/// publishes the recovery handle.
fn recovery_not_ready() -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(ErrorResponse {
            error: "Recovery not ready yet (conductor still starting)".to_string(),
        }),
    )
}

/// Refuses a capital route until startup completes, with the same 503 as
/// `process-tx`: the routes sign with the bot's wallets, and so do the jobs
/// `transfer-usdc` and `transfer-equity` enqueue. The recovery handle and the
/// wallets exist before the startup preflights (each chain's id, the
/// inventory `OPERATOR_ROLE`) have passed, and `health.is_ready()` gates on
/// all of them.
fn require_startup_complete(state: &AppState, route: &'static str) -> Result<(), OpsError> {
    if state.health.is_ready() {
        return Ok(());
    }

    warn!(route, "Capital route refused: startup has not completed");
    Err((
        StatusCode::SERVICE_UNAVAILABLE,
        Json(ErrorResponse {
            error: format!("{route} is unavailable until startup completes"),
        }),
    ))
}

/// The bot's signing wallets. A missing `[wallet]` is a server fault, so the
/// detail is logged and the caller gets a generic 500.
fn bot_wallets(state: &AppState) -> Result<&OnchainWalletCtx, (StatusCode, Json<ErrorResponse>)> {
    state.ctx.wallet().map_err(|error| {
        error!(%error, "Capital route refused: the bot has no signing wallet");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "The bot has no signing wallet configured".to_string(),
            }),
        )
    })
}

/// The chain a vault or allowance route acts on: its trading table and the
/// bot's signer there, resolved like the CLI's `hedged_chain_context`. The
/// trading table is checked first, so a chain without one is a 400 even when
/// no `[wallet]` is configured.
fn hedged_chain_signer(
    state: &AppState,
    chain: Chain,
) -> Result<(&HedgedChain, Arc<dyn Wallet<Provider = RootProvider>>), OpsError> {
    let Some(trading) = state.ctx.chains.hedged_chain(chain) else {
        return Err(ops_precondition_error(format!(
            "{chain} has no [chains.{chain}.trading] table: vault operations need \
             that chain's orderbook, and the primary's addresses do not apply there"
        )));
    };

    Ok((trading, chain_signer(state, chain)?))
}

/// The bot's signer on `chain`. A chain without a signer (Robinhood without
/// its optional one) is refused rather than signed with another chain's
/// wallet, as in the CLI; it is a gap in the bot's config, so it is a 500
/// like a missing wallet.
fn chain_signer(
    state: &AppState,
    chain: Chain,
) -> Result<Arc<dyn Wallet<Provider = RootProvider>>, OpsError> {
    let wallet = crate::conductor::chain_wallet(bot_wallets(state)?, chain).map_err(|error| {
        error!(%chain, ?error, "Capital route refused: the bot has no signer for the chain");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("The bot has no signer for {chain}"),
            }),
        )
    })?;

    Ok(Arc::clone(wallet))
}

/// The chain a wrap, unwrap or redeem acts on and the bot's signer there,
/// resolved like the CLI's `wrap_context`: the chain's trading table must list
/// `symbol`, whose underlying and wrapper addresses come from that listing.
/// A chain without the table, or a symbol it does not list, is a 422, as for
/// the listing checks of `transfer-equity`. Both are config checks, made
/// before the wallet is required.
fn equity_chain_signer<'state>(
    state: &'state AppState,
    chain: Chain,
    symbol: &Symbol,
) -> Result<
    (
        &'state HedgedChain,
        Arc<dyn Wallet<Provider = RootProvider>>,
    ),
    OpsError,
> {
    let Some(trading) = state.ctx.chains.hedged_chain(chain) else {
        return Err(unprocessable(format!(
            "{chain} has no [chains.{chain}.trading] table, so it lists no tokenized \
             equity or wrapper"
        )));
    };
    let listed = &trading.assets.equities.symbols;
    if !listed.contains_key(symbol) {
        let mut available: Vec<String> = listed.keys().map(ToString::to_string).collect();
        available.sort();
        return Err(unprocessable(format!(
            "{symbol} is not listed in [chains.{chain}.trading.assets.equities]; listed: [{}]",
            available.join(", ")
        )));
    }

    Ok((trading, chain_signer(state, chain)?))
}

fn unprocessable(message: String) -> OpsError {
    (
        StatusCode::UNPROCESSABLE_ENTITY,
        Json(ErrorResponse { error: message }),
    )
}

fn conflict(message: String) -> OpsError {
    (StatusCode::CONFLICT, Json(ErrorResponse { error: message }))
}

/// One lock per symbol, shared by `wrap-equity`, `unwrap-equity`, `redeem`
/// and `drain-equity`; see `AppState::equity_wallet_locks`. The map grows only by the
/// symbols a chain lists, since the routes check the listing first.
#[derive(Default)]
pub(crate) struct EquityWalletLocks(parking_lot::Mutex<HashMap<Symbol, Arc<Mutex<()>>>>);

impl EquityWalletLocks {
    /// `symbol`'s lock for a detached task to own, or `None` while another
    /// request holds it.
    pub(crate) fn try_lock(&self, symbol: &Symbol) -> Option<OwnedMutexGuard<()>> {
        let lock = Arc::clone(self.0.lock().entry(symbol.clone()).or_default());
        lock.try_lock_owned().ok()
    }
}

/// What a wrap, unwrap or redeem holds from before its first read until its
/// tx's fate is proven: the symbol's lock, and the bot's equity transfer
/// guard for the symbol, so the bot starts no mint or redemption of the
/// symbol while the operator moves its shares in the wallet.
struct EquityWalletClaim {
    _lock: OwnedMutexGuard<()>,
    _bot_guard: InProgressGuard,
}

/// Takes the symbol's lock, then the bot's equity transfer guard for it,
/// refusing with 409 when either is held: another wrap, unwrap or redeem of
/// the symbol whose outcome is not proven yet, or a transfer or recovery the
/// bot runs on it.
fn claim_equity_wallet(
    locks: &EquityWalletLocks,
    rebalancing: &RebalancingService,
    symbol: &Symbol,
    chain: Chain,
    route: &'static str,
) -> Result<EquityWalletClaim, OpsError> {
    let Some(lock) = locks.try_lock(symbol) else {
        warn!(route, %chain, %symbol, "Equity wallet operation refused: the symbol's lock is held");
        return Err(conflict(format!(
            "Another wrap, unwrap, redeem or drain of {symbol} is in progress or its outcome is \
             unknown; check the bot logs for it before retrying"
        )));
    };
    let Some(bot_guard) = rebalancing.try_claim_equity_guard_for_transfer(symbol) else {
        warn!(
            route, %chain, %symbol,
            "Equity wallet operation refused: the bot's equity transfer guard is held"
        );
        return Err(conflict(format!(
            "The bot is moving {symbol} between Alpaca and Raindex or holds it for a \
             recovery; retry once that transfer has finished"
        )));
    };

    Ok(EquityWalletClaim {
        _lock: lock,
        _bot_guard: bot_guard,
    })
}

/// The gas check of the bot's signer on `chain`, against that chain's own
/// `[alerts.low_balance_thresholds]` entry: `GasReadiness::for_equity_chain`,
/// as the conductor builds the check a manual equity transfer passes, and as
/// `st0x-cli transfer-equity` built it. Built here rather than read from the
/// trigger, which wires one only for the chains that rebalance equity, so a
/// paused chain being drained has none, and only for the corridors that
/// rebalance USDC, whose check also covers the corridor chain's wallet,
/// which `alpaca-deposit` does not use. A missing threshold is a gap in the
/// bot's config, so it is a 500.
fn signer_gas_readiness(
    state: &AppState,
    chain: Chain,
    wallet: &Arc<dyn Wallet<Provider = RootProvider>>,
) -> Result<Arc<GasReadiness>, OpsError> {
    let wallets = bot_wallets(state)?;
    let readiness = state.ctx.alerts.as_ref().map_or_else(
        || {
            Err(anyhow::anyhow!(
                "[alerts] gas thresholds are not configured"
            ))
        },
        |alerts| {
            GasReadiness::for_equity_chain(
                alerts,
                chain,
                wallet,
                wallets.base_wallet(),
                wallets.ethereum_wallet(),
            )
        },
    );
    readiness.map_err(|error| {
        error!(%chain, ?error, "Capital route refused: the bot cannot check its signer's gas");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("The bot has no gas threshold for {chain}"),
            }),
        )
    })
}

/// Refuses with 503, as `transfer-equity` does, while the signer the check
/// covers cannot pay gas or its balance cannot be read.
async fn ensure_signer_gas(
    readiness: &GasReadiness,
    route: &'static str,
    chain: Chain,
) -> Result<(), OpsError> {
    readiness
        .ensure_ready(TransferGasRoute::Equity)
        .await
        .map_err(|error| {
            warn!(route, %chain, %error, "Capital route refused: the signer is not gas ready");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse {
                    error: format!("The {chain} signing wallet is not gas ready: {error}"),
                }),
            )
        })
}

/// Runs `confirm`, which returns once the tx's fate is proven, and frees
/// `claim` after it. A panic would unwind through the claim and free the lock
/// and the bot's guard while the fate is unknown, so it is caught here, the
/// claim is kept until a restart, and the panic resumes for the watcher's
/// join failure log.
async fn hold_until_proven<Proven>(
    claim: EquityWalletClaim,
    route: &'static str,
    symbol: &Symbol,
    tx: TxHash,
    confirm: impl Future<Output = Proven>,
) -> Proven {
    match std::panic::AssertUnwindSafe(confirm).catch_unwind().await {
        Ok(proven) => {
            drop(claim);
            proven
        }
        Err(panic) => {
            error!(
                route, %symbol, %tx,
                "Equity wallet confirmation panicked; keeping its lock and the bot's guard \
                 until a restart. Check the tx onchain before retrying"
            );
            std::mem::forget(claim);
            std::panic::resume_unwind(panic);
        }
    }
}

/// Which vault operation a vault route runs; each names its route.
#[derive(Clone, Copy)]
enum VaultOperation {
    Deposit,
    Withdraw,
    WithdrawUsdc,
}

impl VaultOperation {
    const fn route(self) -> &'static str {
        match self {
            Self::Deposit => "vault-deposit",
            Self::Withdraw => "vault-withdraw",
            Self::WithdrawUsdc => "vault-withdraw-usdc",
        }
    }
}

/// The vault and amount a vault operation acts on.
struct VaultTarget {
    chain: Chain,
    token: Address,
    vault_id: B256,
    amount: Positive<Float>,
}

/// What a vault operation reports back.
struct VaultOutcome {
    decimals: u8,
    amount_raw: U256,
    tx: TxHash,
}

/// How long a capital route waits before it awaits its tx's confirmation again
/// after an inconclusive outcome (a receipt timeout, a drop report, or an RPC
/// failure). The graceful shutdown drain has its own timeout, so the wait
/// never blocks exit.
#[cfg(not(test))]
const CONFIRM_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(30);

#[cfg(test)]
const CONFIRM_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(50);

/// How long an operator tx that holds what blocks rebalancing (a wrap, an
/// unwrap, a redeem, a drain's vault withdrawal, or an Alpaca deposit) may
/// stay unproven before the bot alerts. An equity wallet tx holds the bot's
/// equity transfer guard for the symbol, so meanwhile rebalancing of that
/// symbol is blocked on every chain; an Alpaca deposit holds the resume lock
/// and the USDC driver pause, so every USDC transfer waits.
const CAPITAL_TX_UNPROVEN_ALERT_AFTER: std::time::Duration =
    std::time::Duration::from_secs(10 * 60);

/// Watches an operator tx whose fate the confirmation loop cannot prove yet,
/// and raises one operational alert once it has stayed unproven past
/// [`CAPITAL_TX_UNPROVEN_ALERT_AFTER`]. A tx that never gets a readable
/// receipt (dropped from the mempool, lost by the node) would otherwise block
/// rebalancing silently until a restart.
struct UnprovenCapitalTx {
    since: tokio::time::Instant,
    alerted: bool,
}

impl UnprovenCapitalTx {
    const fn new(since: tokio::time::Instant) -> Self {
        Self {
            since,
            alerted: false,
        }
    }

    /// Records one more unproven round at `now` and returns how long the tx
    /// has waited the first time that passes the bound, when the caller
    /// alerts; `None` before the bound and after the first alert.
    fn due(&mut self, now: tokio::time::Instant) -> Option<std::time::Duration> {
        let waited = now.saturating_duration_since(self.since);
        if self.alerted || waited < CAPITAL_TX_UNPROVEN_ALERT_AFTER {
            return None;
        }
        self.alerted = true;
        Some(waited)
    }

    /// Records one more unproven round of an equity wallet tx at `now`;
    /// alerts the first time the wait passes the bound and returns whether
    /// it alerted.
    fn observe(
        &mut self,
        now: tokio::time::Instant,
        route: &'static str,
        chain: Chain,
        symbol: &Symbol,
        tx: TxHash,
    ) -> bool {
        let Some(waited) = self.due(now) else {
            return false;
        };
        error!(
            target: "operational_alert",
            alert = true,
            kind = AlertKind::CapitalEquityTxUnproven.as_str(),
            route, %chain, %symbol, %tx, waited_secs = waited.as_secs(),
            "An operator equity wallet tx is still unproven; rebalancing of the symbol is \
             blocked on every chain until its fate is known or the bot restarts. Check the tx \
             onchain"
        );
        true
    }

    /// Records one more unproven round of an Alpaca deposit transfer at
    /// `now`; alerts the first time the wait passes the bound.
    fn observe_alpaca_deposit(
        &mut self,
        now: tokio::time::Instant,
        deposit_address: Address,
        tx: TxHash,
    ) {
        let Some(waited) = self.due(now) else {
            return;
        };
        error!(
            target: "operational_alert",
            alert = true,
            kind = AlertKind::CapitalAlpacaDepositUnproven.as_str(),
            route = ALPACA_DEPOSIT_ROUTE, %deposit_address, %tx, waited_secs = waited.as_secs(),
            "An operator Alpaca deposit transfer is still unproven; every USDC transfer waits \
             until its fate is known, across restarts. Check the tx onchain: the bot keeps \
             sending its same bytes, and frees the wait once it mines or another tx takes its \
             nonce"
        );
    }
}

/// What the confirmation attempt of a tx a capital route sent proved.
#[derive(Debug, PartialEq, Eq)]
enum TxFate {
    Confirmed,
    /// The tx mined and failed: a status 0 receipt at the chain's required
    /// confirmations, or a decoded revert, which the wallet only produces from
    /// such a receipt.
    Failed,
    /// Nothing proven: the tx may still mine. A drop report is in here too:
    /// the wallet calls a tx dropped when the node it asks has no receipt and
    /// no pending tx for it, which a lagging or load balanced node also says
    /// of a tx another node still holds.
    Unknown,
}

/// The fate `mined`, a tx's own canonical receipt as the node shows it now,
/// proves on its own: it decides whenever it has `required_confirmations`, so
/// a revert whose replay cannot be decoded (no revert data, or pruned state)
/// still ends the wait.
fn receipt_fate(mined: Option<&MinedTx>, required_confirmations: u64) -> Option<TxFate> {
    let mined = mined.filter(|mined| mined.confirmations >= required_confirmations)?;
    Some(if mined.succeeded {
        TxFate::Confirmed
    } else {
        TxFate::Failed
    })
}

/// Decides a vault tx's fate after `confirm_tx` failed with `error`: its
/// receipt decides first ([`receipt_fate`]), then the error.
fn vault_tx_fate(
    error: &RaindexError,
    mined: Option<&MinedTx>,
    required_confirmations: u64,
) -> TxFate {
    if let Some(fate) = receipt_fate(mined, required_confirmations) {
        return fate;
    }
    match error {
        RaindexError::Evm(evm) if evm.is_revert() => TxFate::Failed,
        RaindexError::InsufficientVaultLiquidity { .. } => TxFate::Failed,
        RaindexError::Evm(_)
        | RaindexError::Contract(_)
        | RaindexError::Float(_)
        | RaindexError::ZeroAmount
        | RaindexError::RpcTransport(_)
        | RaindexError::SolType(_)
        | RaindexError::ScanInconclusive { .. }
        | RaindexError::ScanAnomalousLog { .. }
        | RaindexError::MissingOperatorRole { .. }
        | RaindexError::WithdrawReceiptNotVisible { .. } => TxFate::Unknown,
    }
}

/// Decides a wrap or unwrap tx's fate after its confirmation failed with
/// `error`, like [`vault_tx_fate`]. A receipt the wrapper read but could not
/// account for (no `Deposit` or `Withdraw` event, or no transfer of the
/// underlying to the receiver) also ends the wait: the tx mined, and waiting
/// again cannot change its receipt.
fn wrapper_tx_fate(
    error: &WrapperError,
    mined: Option<&MinedTx>,
    required_confirmations: u64,
) -> TxFate {
    if let Some(fate) = receipt_fate(mined, required_confirmations) {
        return fate;
    }
    match error {
        WrapperError::Evm(evm) if evm.is_revert() => TxFate::Failed,
        WrapperError::MissingDepositEvent
        | WrapperError::MissingWithdrawEvent
        | WrapperError::MissingUnderlyingTransfer { .. } => TxFate::Failed,
        WrapperError::Evm(_)
        | WrapperError::SymbolNotConfigured(_)
        | WrapperError::RedeemExceedsMax { .. }
        | WrapperError::VaultAssetMismatch { .. }
        | WrapperError::Contract(_)
        | WrapperError::Ratio(_)
        | WrapperError::MissingBlockNumber { .. } => TxFate::Unknown,
    }
}

/// Decides a redemption transfer's fate after its confirmation failed with
/// `error`, like [`vault_tx_fate`]. A reverted transfer is reported by the
/// confirmation itself, not as an error.
fn redemption_send_fate(
    error: &TokenizerError,
    mined: Option<&MinedTx>,
    required_confirmations: u64,
) -> TxFate {
    if let Some(fate) = receipt_fate(mined, required_confirmations) {
        return fate;
    }
    match error {
        TokenizerError::Evm(evm) if evm.is_revert() => TxFate::Failed,
        TokenizerError::Evm(_)
        | TokenizerError::Alpaca(_)
        | TokenizerError::MissingRedemptionWallet
        | TokenizerError::MintVerification(_) => TxFate::Unknown,
    }
}

/// Reads the token's decimals, scales the amount to the token's smallest unit
/// like the CLI's `float_to_u256`, and runs the operation through the chain's
/// `RaindexService`, built from the bot's signer like the startup revoke. The
/// reads run in the detached task too, so the whole sequence is one tracked
/// unit.
///
/// Answers as soon as the vault transaction is broadcast, like `cctp_bridge`:
/// the chain's required confirmations (12 on Ethereum in production) outlast
/// the 60 second load balancer cut. The task then awaits the confirmation
/// through `Raindex::confirm_tx`, as the USDC transfer worker does after its
/// own `submit_deposit`, waits again while the outcome is inconclusive, and
/// logs the outcome. The verb's lock is held until then. A deposit still
/// awaits its approve's confirmation before the broadcast when the allowance
/// is short, so only a deposit of a token without the startup MAX grant can
/// still wait that long.
async fn run_vault_operation(
    state: &AppState,
    operation: VaultOperation,
    trading: &HedgedChain,
    wallet: Arc<dyn Wallet<Provider = RootProvider>>,
    target: VaultTarget,
) -> Result<VaultOutcome, (StatusCode, Json<ErrorResponse>)> {
    let raindex = RaindexService::new(
        Arc::clone(&wallet),
        crate::onchain::raindex_contracts(trading),
        wallet.address(),
    );
    let required_confirmations = trading.required_confirmations;
    let VaultTarget {
        chain,
        token,
        vault_id,
        amount,
    } = target;
    let route = operation.route();

    // Each vault verb holds its lock from before its first read until its tx's
    // fate is proven (confirmed, or mined and failed), inside the task so a
    // dropped request cannot release it early; see `AppState::vault_deposit_lock`
    // and `AppState::vault_withdraw_lock`. A tx that never gets a receipt, or a
    // panic while confirming, leaves the fate unknown, so it keeps the lock
    // until a restart. A failed send frees it, like `st0x-cli`: its error
    // cannot tell whether the tx went out, which `onchain_failure` tells the
    // operator to check before retrying.
    let (lock, refusal) = match operation {
        VaultOperation::Deposit => (
            Arc::clone(&state.vault_deposit_lock),
            "Another vault deposit is in progress or its outcome is unknown; check the bot \
             logs for it before retrying",
        ),
        VaultOperation::Withdraw | VaultOperation::WithdrawUsdc => (
            Arc::clone(&state.vault_withdraw_lock),
            "Another vault withdrawal is in progress or its outcome is unknown; check the bot \
             logs for it before retrying",
        ),
    };
    answer_from_detached(&state.detached_tasks, route, token, async move {
        let vault_guard = lock.try_lock_owned().map_err(|_| {
            warn!(route, %chain, %token, "Vault operation refused: {refusal}");
            (
                StatusCode::CONFLICT,
                Json(ErrorResponse {
                    error: refusal.to_string(),
                }),
            )
        })?;
        let decimals = wallet
            .call::<OpenChainErrorRegistry, _>(token, IERC20::decimalsCall {})
            .await
            .map_err(|error| onchain_failure(route, chain, &error))?;
        let amount_raw = amount.inner().to_fixed_decimal(decimals).map_err(|error| {
            warn!(route, %chain, %token, ?error, "Vault amount does not fit the token's decimals");
            ops_precondition_error(format!(
                "amount {} does not fit the token's {decimals} decimals: {error}",
                format_float_with_fallback(&amount.inner())
            ))
        })?;

        let submitted = match operation {
            VaultOperation::Deposit => {
                raindex
                    .submit_deposit(token, RaindexVaultId(vault_id), amount_raw, decimals)
                    .await
            }
            VaultOperation::Withdraw | VaultOperation::WithdrawUsdc => {
                raindex
                    .submit_withdraw(token, RaindexVaultId(vault_id), amount_raw, decimals)
                    .await
            }
        };
        let tx = submitted.map_err(|error| onchain_failure(route, chain, &error))?;
        info!(route, %chain, %token, %vault_id, %amount_raw, %tx, "Vault operation broadcast via API");

        let confirm = async move {
            let wait = confirm_vault_tx(
                &raindex,
                VaultTx {
                    route,
                    chain,
                    token,
                    vault_id,
                    amount_raw,
                    tx,
                    required_confirmations,
                },
                None,
            );
            // A panic would unwind through the guard and free the lock while
            // the tx's fate is unknown, so it is caught here, the guard is
            // kept until a restart, and the panic resumes for the watcher's
            // join failure log.
            match std::panic::AssertUnwindSafe(wait).catch_unwind().await {
                Ok(_confirmed) => drop(vault_guard),
                Err(panic) => {
                    error!(
                        route, %chain, %token, %vault_id, %amount_raw, %tx,
                        "Vault operation confirmation panicked; keeping its lock until a \
                         restart. Check the tx onchain before retrying"
                    );
                    std::mem::forget(vault_guard);
                    std::panic::resume_unwind(panic);
                }
            }
        };
        let outcome = VaultOutcome {
            decimals,
            amount_raw,
            tx,
        };
        Ok::<_, OpsError>((outcome, confirm))
    })
    .await
}

/// A vault tx a capital route sent, as its confirmation loop needs it.
#[derive(Clone, Copy)]
struct VaultTx {
    route: &'static str,
    chain: Chain,
    token: Address,
    vault_id: B256,
    amount_raw: U256,
    tx: TxHash,
    required_confirmations: u64,
}

/// Awaits a vault tx's confirmation through `Raindex::confirm_tx`, as the USDC
/// transfer worker does after its own submit, and returns whether it
/// confirmed. Its own receipt decides when the error does not, and the loop
/// waits again while the fate is unknown: the tx can still land, and giving up
/// would free a lock a rerun needs to stay held. When the wait holds `held`'s
/// equity transfer guard, a fate that stays unknown alerts once
/// ([`UnprovenCapitalTx`]).
async fn confirm_vault_tx(
    raindex: &impl Raindex,
    vault_tx: VaultTx,
    held: Option<&Symbol>,
) -> bool {
    let VaultTx {
        route,
        chain,
        token,
        vault_id,
        amount_raw,
        tx,
        required_confirmations,
    } = vault_tx;
    let mut unproven = UnprovenCapitalTx::new(tokio::time::Instant::now());
    loop {
        let error = match raindex.confirm_tx(tx).await {
            Ok(()) => {
                info!(
                    route, %chain, %token, %vault_id, %amount_raw, %tx,
                    "Vault operation confirmed via API"
                );
                return true;
            }
            Err(error) => error,
        };
        // The tx's own receipt decides when the error does not: a revert
        // whose replay cannot be decoded still mined.
        let mined = raindex.mined_tx(tx).await.unwrap_or_else(|read_error| {
            warn!(route, %chain, %tx, ?read_error, "Could not read the vault tx's receipt");
            None
        });
        match vault_tx_fate(&error, mined.as_ref(), required_confirmations) {
            TxFate::Confirmed => {
                info!(
                    route, %chain, %token, %vault_id, %amount_raw, %tx, ?error,
                    "Vault operation confirmed via API"
                );
                return true;
            }
            TxFate::Failed => {
                error!(
                    route, %chain, %token, %vault_id, %amount_raw, %tx, ?error,
                    "Vault operation broadcast via API did not confirm"
                );
                return false;
            }
            // A receipt timeout, a drop report, or an RPC failure proves
            // nothing: the tx can still land, and releasing the lock would let
            // a rerun send a second one.
            TxFate::Unknown => {
                warn!(
                    route, %chain, %token, %vault_id, %amount_raw, %tx, ?error,
                    "Vault operation broadcast via API is not confirmed yet; keeping its lock \
                     and waiting again"
                );
                if let Some(symbol) = held {
                    unproven.observe(tokio::time::Instant::now(), route, chain, symbol, tx);
                }
                tokio::time::sleep(CONFIRM_RETRY_DELAY).await;
            }
        }
    }
}

/// Records an onchain failure inside a capital route's detached task, where
/// the record survives a dropped request, and renders a generic 500. A failed
/// send may still have landed, so the message points at the chain.
fn onchain_failure(
    route: &'static str,
    chain: Chain,
    error: &impl Debug,
) -> (StatusCode, Json<ErrorResponse>) {
    error!(route, %chain, ?error, "Capital route failed onchain");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse {
            error: format!(
                "{route} failed on {chain}; check the bot logs and the chain before \
                 retrying"
            ),
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use st0x_evm::EvmError;

    fn mined(succeeded: bool, confirmations: u64) -> MinedTx {
        MinedTx {
            from: Address::ZERO,
            to: Some(Address::ZERO),
            nonce: 0,
            value: U256::ZERO,
            input: alloy::primitives::Bytes::new(),
            tx_type: 2,
            succeeded,
            emitted_logs: succeeded,
            confirmations,
        }
    }

    /// An operator equity wallet tx that stays unproven pages once, only after
    /// the bound, with the symbol, the chain and the tx, so a dropped tx that
    /// blocks the symbol's rebalancing is never silent and never repeats.
    #[tracing_test::traced_test]
    #[test]
    fn an_unproven_equity_wallet_tx_alerts_once_past_the_bound() {
        let since = tokio::time::Instant::now();
        let mut unproven = UnprovenCapitalTx::new(since);
        let symbol = Symbol::new("AAPL").unwrap();
        let tx = TxHash::repeat_byte(0x42);
        let observe = |unproven: &mut UnprovenCapitalTx, after: std::time::Duration| {
            unproven.observe(since + after, "unwrap-equity", Chain::Base, &symbol, tx)
        };

        assert!(!observe(&mut unproven, CONFIRM_RETRY_DELAY));
        assert!(!observe(
            &mut unproven,
            CAPITAL_TX_UNPROVEN_ALERT_AFTER.saturating_sub(std::time::Duration::from_secs(1))
        ));
        assert!(!logs_contain("still unproven"));

        assert!(observe(&mut unproven, CAPITAL_TX_UNPROVEN_ALERT_AFTER));
        assert!(!observe(
            &mut unproven,
            CAPITAL_TX_UNPROVEN_ALERT_AFTER + CONFIRM_RETRY_DELAY
        ));
        logs_assert(|lines: &[&str]| {
            let alerts: Vec<_> = lines
                .iter()
                .filter(|line| line.contains("An operator equity wallet tx is still unproven"))
                .collect();
            match alerts.as_slice() {
                [line]
                    if line.contains("operational_alert")
                        && line.contains("AAPL")
                        && line.contains("base")
                        && line.contains(&tx.to_string()) =>
                {
                    Ok(())
                }
                other => Err(format!(
                    "expected one alert naming AAPL, base and the tx: {other:?}"
                )),
            }
        });
    }

    /// The audit record names what a body-only capital route acted on, from
    /// the wire body the CLI sends; extra fields never reach it.
    #[test]
    fn capital_bodies_name_their_audit_targets() {
        let vault: VaultRequest = serde_json::from_value(serde_json::json!({
            "chain": "base",
            "token": "0x1111111111111111111111111111111111111111",
            "vaultId": format!("0x{}", "22".repeat(32)),
            "amount": "1",
            "vault_id": "forged alias",
        }))
        .unwrap();
        assert_eq!(
            vault.audit_targets(),
            [
                "base".to_string(),
                "0x1111111111111111111111111111111111111111".to_string(),
                format!("0x{}", "22".repeat(32)),
            ]
        );
        assert_eq!(vault.audit_reason(), None);

        let bridge: CctpBridgeRequest = serde_json::from_value(serde_json::json!({
            "operationId": "33333333-3333-3333-3333-333333333333",
            "from": "ethereum",
            "all": true,
        }))
        .unwrap();
        assert_eq!(
            bridge.audit_targets(),
            ["33333333-3333-3333-3333-333333333333", "ethereum"]
        );

        let equity: TransferEquityRequest = serde_json::from_value(serde_json::json!({
            "direction": "to-alpaca",
            "symbol": "AAPL",
            "quantity": "1.5",
            "chain": "base",
            "network": "forged alias",
        }))
        .unwrap();
        assert_eq!(equity.audit_targets(), ["to-alpaca", "AAPL", "base"]);
    }

    /// The audit record spells the transfer direction by hand. It must equal
    /// serde's wire name, so a renamed variant cannot leave the record on the
    /// old string. The empty match stops compiling when a variant is added,
    /// so the list cannot fall behind the enum.
    #[test]
    fn transfer_direction_audit_spelling_matches_the_serde_wire_name() {
        for direction in [
            TransferDirectionWire::ToRaindex,
            TransferDirectionWire::ToAlpaca,
        ] {
            match direction {
                TransferDirectionWire::ToRaindex | TransferDirectionWire::ToAlpaca => {}
            }
            let request = TransferUsdcRequest {
                direction,
                amount: "1".to_owned(),
                chain: None,
            };
            assert_eq!(
                serde_json::json!(request.audit_targets()[0]),
                serde_json::to_value(direction).unwrap()
            );
        }
    }

    fn dropped() -> RaindexError {
        RaindexError::Evm(EvmError::TransactionDropped {
            tx_hash: TxHash::ZERO,
            elapsed_secs: 0,
        })
    }

    fn unreplayable() -> RaindexError {
        RaindexError::Evm(EvmError::Transport(alloy::transports::RpcError::ErrorResp(
            alloy::rpc::json_rpc::ErrorPayload {
                code: -32000,
                message: "missing trie node".into(),
                data: None,
            },
        )))
    }

    /// A drop report proves nothing while the node shows no receipt: the tx
    /// may still be pending on another node.
    #[test]
    fn a_drop_report_without_a_receipt_leaves_the_fate_unknown() {
        assert_eq!(vault_tx_fate(&dropped(), None, 3), TxFate::Unknown);
    }

    /// The tx's own receipt at the required depth decides over the error,
    /// whichever way it went.
    #[test]
    fn a_receipt_at_the_required_depth_decides_the_fate() {
        assert_eq!(
            vault_tx_fate(&dropped(), Some(&mined(true, 3)), 3),
            TxFate::Confirmed
        );
        assert_eq!(
            vault_tx_fate(&unreplayable(), Some(&mined(false, 3)), 3),
            TxFate::Failed
        );
    }

    /// A receipt short of the required depth can still be reorged out, so
    /// only a decoded revert decides then.
    #[test]
    fn a_shallow_receipt_does_not_decide_the_fate() {
        assert_eq!(
            vault_tx_fate(&unreplayable(), Some(&mined(false, 2)), 3),
            TxFate::Unknown
        );
        assert_eq!(
            vault_tx_fate(&dropped(), Some(&mined(true, 2)), 3),
            TxFate::Unknown
        );
        assert_eq!(
            vault_tx_fate(
                &RaindexError::Evm(EvmError::Reverted {
                    tx_hash: TxHash::ZERO
                }),
                None,
                3
            ),
            TxFate::Failed
        );
    }

    #[test]
    fn transfer_request_parses_the_cli_direction_spelling_and_a_string_amount() {
        let request: TransferUsdcRequest = serde_json::from_value(
            serde_json::json!({"direction": "to-raindex", "amount": "250.5"}),
        )
        .unwrap();
        assert_eq!(
            RebalanceDirection::from(request.direction),
            RebalanceDirection::AlpacaToBase
        );
        assert_eq!(request.amount, "250.5");

        let request: TransferUsdcRequest =
            serde_json::from_value(serde_json::json!({"direction": "to-alpaca", "amount": "1"}))
                .unwrap();
        assert_eq!(
            RebalanceDirection::from(request.direction),
            RebalanceDirection::BaseToAlpaca
        );

        for direction in ["alpaca_to_base", "to_raindex", "ToRaindex"] {
            let Err(error) = serde_json::from_value::<TransferUsdcRequest>(
                serde_json::json!({"direction": direction, "amount": "1"}),
            ) else {
                panic!("{direction} must not parse");
            };
            assert_eq!(error.classify(), serde_json::error::Category::Data);
            assert!(
                error
                    .to_string()
                    .starts_with(&format!("unknown variant `{direction}`")),
                "{direction}: {error}"
            );
        }
    }

    #[test]
    fn transfer_response_echoes_the_wire_direction_in_camel_case() {
        let json = serde_json::to_value(TransferUsdcResponse {
            transfer_id: "id".to_string(),
            direction: TransferDirectionWire::ToRaindex,
            amount: "250.5".to_string(),
            outcome: "enqueued",
        })
        .unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "transferId": "id",
                "direction": "to-raindex",
                "amount": "250.5",
                "outcome": "enqueued",
            })
        );
    }

    /// The equity response is the client contract: camelCase keys, the wire
    /// direction, and the chain by its wire name.
    #[test]
    fn transfer_equity_response_follows_the_wire_contract() {
        let json = serde_json::to_value(TransferEquityResponse {
            transfer_id: "id".to_string(),
            direction: TransferDirectionWire::ToAlpaca,
            symbol: "AAPL".to_string(),
            quantity: "1.5".to_string(),
            chain: Chain::Base,
            outcome: "enqueued",
        })
        .unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "transferId": "id",
                "direction": "to-alpaca",
                "symbol": "AAPL",
                "quantity": "1.5",
                "chain": "base",
                "outcome": "enqueued",
            })
        );
    }

    #[test]
    fn vault_request_reads_camel_case_fields() {
        let request: VaultRequest = serde_json::from_value(serde_json::json!({
            "chain": "hyperevm",
            "token": "0x0000000000000000000000000000000000000001",
            "vaultId": "0x0000000000000000000000000000000000000000000000000000000000000002",
            "amount": "1.5",
        }))
        .unwrap();
        assert_eq!(request.chain, Chain::HyperEvm);
        assert_eq!(request.vault_id, B256::with_last_byte(2));
        assert_eq!(request.amount, "1.5");

        let Err(error) = serde_json::from_value::<VaultRequest>(serde_json::json!({
            "chain": "base",
            "token": "0x0000000000000000000000000000000000000001",
            "vault_id": "0x0000000000000000000000000000000000000000000000000000000000000002",
            "amount": "1.5",
        })) else {
            panic!("a snake_case vault id must not parse");
        };
        assert_eq!(error.classify(), serde_json::error::Category::Data);
        assert_eq!(error.to_string(), "missing field `vaultId`");
    }

    #[test]
    fn positive_amount_refuses_zero_negative_and_unparseable_amounts() {
        for amount in ["0", "-1", "abc", ""] {
            let Err((status, _)) = positive_amount(amount, Usdc::new) else {
                panic!("{amount:?} must be refused");
            };
            assert_eq!(status, StatusCode::BAD_REQUEST, "{amount:?}");
        }

        let amount = positive_amount("250.5", Usdc::new).unwrap();
        assert_eq!(amount.inner().to_string(), "250.5");
    }

    /// Six decimals is USDC's whole grid: the smallest unit converts exactly
    /// and one digit finer is input the token cannot hold.
    #[test]
    fn positive_usdc_refuses_an_amount_finer_than_six_decimals() {
        let (amount, raw) = positive_usdc("250.000001").unwrap();
        assert_eq!(amount.inner().to_string(), "250.000001");
        assert_eq!(raw, U256::from(250_000_001_u64));

        for amount in ["0.0000001", "250.0000001"] {
            let Err((status, Json(body))) = positive_usdc(amount) else {
                panic!("{amount} must be refused");
            };
            assert_eq!(status, StatusCode::BAD_REQUEST, "{amount}: {}", body.error);
            assert!(
                body.error
                    .starts_with(&format!("invalid USDC amount {amount}")),
                "{amount}: {}",
                body.error
            );
        }
    }

    #[test]
    fn burn_amount_takes_exactly_one_of_amount_and_all() {
        assert_eq!(
            burn_amount(Some("100"), false).unwrap(),
            RequestedBurn::Exact {
                amount: U256::from(100_000_000_u64)
            }
        );
        assert_eq!(burn_amount(None, true).unwrap(), RequestedBurn::All);

        for (amount, all) in [(Some("100"), true), (None, false), (Some("0"), false)] {
            let Err((status, _)) = burn_amount(amount, all) else {
                panic!("amount {amount:?} with all {all} must be refused");
            };
            assert_eq!(status, StatusCode::BAD_REQUEST);
        }
    }

    /// The operation id is what makes a retry safe, so a request without one
    /// is refused rather than burning untracked.
    #[test]
    fn cctp_request_needs_an_operation_id_and_reads_from_and_either_amount_or_all() {
        let id = "6f1c2a1e-6c39-4a77-9a8e-1f0b7d6e8c11";
        let request: CctpBridgeRequest = serde_json::from_value(
            serde_json::json!({"operationId": id, "from": "base", "all": true}),
        )
        .unwrap();
        assert!(request.all);
        assert_eq!(request.amount, None);
        assert_eq!(request.operation_id.to_string(), id);
        assert_eq!(request.from, CctpSourceChain::Base);

        let request: CctpBridgeRequest = serde_json::from_value(
            serde_json::json!({"operationId": id, "from": "ethereum", "amount": "100"}),
        )
        .unwrap();
        assert!(!request.all);
        assert_eq!(request.amount.as_deref(), Some("100"));
        assert_eq!(request.from, CctpSourceChain::Ethereum);

        let missing_id = serde_json::from_value::<CctpBridgeRequest>(
            serde_json::json!({"from": "base", "all": true}),
        );
        assert!(matches!(missing_id, Err(error) if error.to_string().contains("operationId")));
    }

    #[test]
    fn reset_allowance_outcomes_use_snake_case() {
        assert_eq!(
            serde_json::to_value(ResetAllowanceOutcome::Revoked).unwrap(),
            serde_json::json!("revoked")
        );
        assert_eq!(
            serde_json::to_value(ResetAllowanceOutcome::AlreadyZero).unwrap(),
            serde_json::json!("already_zero")
        );
    }
}

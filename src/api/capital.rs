//! The capital group of the ops write API: typed routes for the capital verbs
//! of `st0x-cli`, run inside the bot so they sign with its own wallets and
//! share their nonce state instead of building a second signer.
//!
//! No route waits on CCTP attestation or on USDC settlement, since the ops
//! load balancer times out first: `transfer-usdc` enqueues the transfer on
//! the bot's own worker and returns its id, and `cctp-bridge` returns its burn
//! tx at the broadcast. The routes that send transactions, and the
//! `transfer-usdc` enqueue, run through `spawn_detached`, so a dropped request
//! cannot cancel them midway (between broadcast and receipt, or between the
//! enqueue and the corridor claim it keeps), and graceful shutdown waits for
//! them. Every capital route refuses until startup completes, like
//! `process-tx`.

use std::fmt::Debug;
use std::sync::Arc;

use alloy::primitives::{Address, B256, TxHash, U256};
use alloy::providers::RootProvider;
use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use rain_math_float::Float;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use st0x_bridge::Bridge;
use st0x_bridge::cctp::{CctpBridge, CctpCtx};
use st0x_config::{HedgedChain, OnchainWalletCtx};
use st0x_evm::{Chain, Evm, IERC20, OpenChainErrorRegistry, Wallet};
use st0x_finance::{HasZero, Positive, Usdc};
use st0x_float_serde::format_float_with_fallback;
use st0x_raindex::{Raindex, RaindexService, RaindexVaultId, RevokeOutcome};

use super::{
    CctpSourceChain, ErrorResponse, OpsError, UsdcDriverPauseRequest, ops_precondition_error,
    quiesce_usdc_driver, spawn_detached, usdc_resume_error_response,
};
use crate::AppState;
use crate::rebalancing::UsdcResumeError;
use crate::usdc_rebalance::RebalanceDirection;

/// The direction of a manual USDC transfer, spelled on the wire like the
/// `st0x-cli` `--direction` values (`to-raindex`, `to-alpaca`).
#[derive(Deserialize, Serialize, Clone, Copy)]
#[serde(rename_all = "kebab-case")]
enum TransferDirectionWire {
    ToRaindex,
    ToAlpaca,
}

impl From<TransferDirectionWire> for RebalanceDirection {
    fn from(wire: TransferDirectionWire) -> Self {
        match wire {
            TransferDirectionWire::ToRaindex => Self::AlpacaToBase,
            TransferDirectionWire::ToAlpaca => Self::BaseToAlpaca,
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
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct TransferUsdcResponse {
    transfer_id: String,
    direction: TransferDirectionWire,
    amount: String,
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

/// Wire contract for the vault withdraw USDC route: the chain's settlement
/// stable out of its first configured cash vault.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct VaultWithdrawUsdcRequest {
    chain: Chain,
    /// Decimal amount of the settlement stable; must be positive.
    amount: String,
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

/// Wire contract for the CCTP burn route: exactly one of `amount` and
/// `all: true`.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct CctpBridgeRequest {
    from: CctpSourceChain,
    #[serde(default)]
    amount: Option<String>,
    #[serde(default)]
    all: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct CctpBridgeResponse {
    burn_tx: TxHash,
    source_chain: Chain,
    destination_chain: Chain,
    /// The burned amount in USDC base units (6 decimals).
    amount_raw: String,
}

/// Wire contract for the orderbook allowance reset route.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ResetAllowanceRequest {
    chain: Chain,
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
    Json(request): Json<TransferUsdcRequest>,
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
            .start_manual_usdc_transfer(&state.pool, direction, amount)
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

/// Deposits `amount` of `token` into a Raindex vault on the chosen chain:
/// approve then deposit, with the token's decimals read onchain. Mirrors
/// `st0x-cli vault-deposit`.
pub(super) async fn vault_deposit(
    State(state): State<AppState>,
    Json(request): Json<VaultRequest>,
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
    Json(request): Json<VaultRequest>,
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
    Json(request): Json<VaultWithdrawUsdcRequest>,
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
/// Like `complete_cctp_mint`, the recovery handle is checked before the
/// resume lock, and the lock and the driver pause are held around the work
/// that spends the rebalancing wallet's USDC, from the balance read through
/// the burn's confirmation. Both are taken inside the detached task, so they
/// stay held until the burn confirms even when the request is dropped.
///
/// The task answers the request at the broadcast, not at the receipt: an
/// approve plus the burn's confirmations can outlast the load balancer's 60
/// second cut, and a request that times out after the broadcast would leave
/// the operator without the one value `cctp complete-mint` needs, inviting a
/// retry that burns again. The task then awaits the receipt and logs whether
/// the burn confirmed. Nothing records the burn, so the route is not
/// idempotent: a retried request burns again (see the operator docs).
pub(super) async fn cctp_bridge(
    State(state): State<AppState>,
    Json(request): Json<CctpBridgeRequest>,
) -> Result<Json<CctpBridgeResponse>, (StatusCode, Json<ErrorResponse>)> {
    let amount = burn_amount(request.amount.as_deref(), request.all)?;
    let from = request.from;
    let direction = from.bridge_direction();
    let (source_chain, destination_chain) = cctp_route(from);

    require_startup_complete(&state, "cctp-bridge")?;
    let handle = state.recovery.get().ok_or_else(recovery_not_ready)?;
    // Every other corridor move proves both wallets can pay gas first: the
    // burn spends the source wallet's gas, and the `complete-mint` it leads to
    // spends the destination wallet's. Refused before anything is locked.
    handle
        .rebalancing_service
        .ensure_usdc_corridor_gas_ready()
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
    let wallets = bot_wallets(&state)?;
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

    let bridge = CctpBridge::try_from_ctx(CctpCtx {
        corridor,
        ethereum_wallet: Arc::clone(wallets.ethereum_wallet()),
        base_wallet: Arc::clone(wallets.base_wallet()),
        // The CLI passes the production constants here; the bot takes its
        // own configured overrides, like the conductor's bridge, so a test
        // deployment's burn reaches the same contracts as its rebalances.
        #[cfg(feature = "test-support")]
        circle_api_base: state.ctx.rebalancing.circle_api_base.clone(),
        #[cfg(feature = "test-support")]
        token_messenger: state.ctx.rebalancing.token_messenger,
        #[cfg(feature = "test-support")]
        message_transmitter: state.ctx.rebalancing.message_transmitter,
    })
    .map_err(|error| {
        error!(
            ?error,
            ?direction,
            "Failed to build the CCTP bridge for a burn"
        );
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "Failed to build the CCTP bridge".to_string(),
            }),
        )
    })?;

    let resume_lock = Arc::clone(&state.resume_lock);
    let driver_pause = Arc::clone(&handle.usdc_driver_pause);
    let (respond, response) =
        tokio::sync::oneshot::channel::<Result<CctpBridgeResponse, OpsError>>();
    let worker = spawn_detached(
        &state.detached_tasks,
        "cctp-bridge",
        source_chain,
        async move {
            let broadcast = async {
                let guard = resume_lock.0.try_lock().map_err(|_| {
                    (
                        StatusCode::CONFLICT,
                        Json(ErrorResponse {
                            error: "A resume or recheck operation is already in progress".to_string(),
                        }),
                    )
                })?;
                let driver_paused = quiesce_usdc_driver(
                    &driver_pause,
                    UsdcDriverPauseRequest::CctpBurn { direction },
                )
                .await?;

                let amount = match &amount {
                    BurnAmount::Exact(amount) => *amount,
                    // Read under the driver pause, so no transfer spends the
                    // balance between this read and the burn.
                    BurnAmount::All => {
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

                let burn_tx = bridge
                    .submit_burn(direction, amount, recipient)
                    .await
                    .map_err(|error| onchain_failure("cctp-bridge", source_chain, &error))?;
                info!(%burn_tx, ?direction, %amount, "CCTP burn broadcast via API");
                Ok::<_, OpsError>((guard, driver_paused, amount, burn_tx))
            }
            .await;

            // The lock and the pause stay held through the confirmation below.
            let (_guard, _driver_paused, amount, burn_tx) = match broadcast {
                Ok(broadcast) => broadcast,
                Err(refusal) => {
                    // A refusal, the balance read, or a reverted send broadcast
                    // nothing. A transport error from `submit_burn` may have
                    // broadcast anyway, which is why `onchain_failure` tells the
                    // operator to check the chain before retrying. A dropped
                    // request has no receiver left.
                    let _ = respond.send(Err(refusal));
                    return;
                }
            };
            let _ = respond.send(Ok(CctpBridgeResponse {
                burn_tx,
                source_chain,
                destination_chain,
                amount_raw: amount.to_string(),
            }));

            match bridge.confirm_burn(direction, burn_tx, amount).await {
                Ok(receipt) => info!(
                    burn_tx = %receipt.tx,
                    ?direction,
                    amount = %receipt.amount,
                    "CCTP burn confirmed via API"
                ),
                Err(error) => error!(
                    %burn_tx,
                    ?direction,
                    %amount,
                    ?error,
                    "CCTP burn broadcast via API did not confirm; check the tx onchain \
                     before completing the mint or retrying"
                ),
            }
        },
    )?;

    // The task answers at the broadcast and keeps running to confirm the burn,
    // so the request cannot be the one to join it: a tracked watcher does, and
    // a panic in either phase still reaches the join failure log of
    // `spawn_detached`, after the response or a dropped request alike.
    state.detached_tasks.spawn(async move {
        let _joined = worker.await;
    });

    match response.await {
        Ok(answer) => answer.map(Json),
        // The task dropped its sender without answering: it panicked before
        // the broadcast, and the watcher logs the join failure.
        Err(_answerless) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "cctp-bridge worker task failed".to_string(),
            }),
        )),
    }
}

/// Zeroes the chosen chain's settlement stable allowance for that chain's
/// orderbook, through `RaindexService::revoke_orderbook_allowance` like the
/// startup revoke. Mirrors `st0x-cli reset-allowance`.
pub(super) async fn reset_allowance(
    State(state): State<AppState>,
    Json(request): Json<ResetAllowanceRequest>,
) -> Result<Json<ResetAllowanceResponse>, (StatusCode, Json<ErrorResponse>)> {
    let ResetAllowanceRequest { chain } = request;
    let (trading, wallet) = hedged_chain_signer(&state, chain)?;
    require_startup_complete(&state, "reset-allowance")?;
    let token = chain.settlement_stable().address;
    let contracts = crate::onchain::raindex_contracts(trading);
    let spender = contracts.orderbook;
    let owner = wallet.address();
    let raindex = RaindexService::new(wallet, contracts, owner);

    let outcome = spawn_detached(
        &state.detached_tasks,
        "reset-allowance",
        chain,
        async move {
            let outcome = raindex
                .revoke_orderbook_allowance::<OpenChainErrorRegistry>(token)
                .await
                .map_err(|error| onchain_failure("reset-allowance", chain, &error))?;
            info!(%chain, %token, %spender, ?outcome, "Orderbook allowance reset via API");
            Ok::<_, (StatusCode, Json<ErrorResponse>)>(outcome)
        },
    )?
    .await??;

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

/// How much a CCTP burn moves.
#[derive(Debug, PartialEq, Eq)]
enum BurnAmount {
    /// The operator's amount in USDC base units.
    Exact(U256),
    /// The source wallet's whole USDC balance, read under the driver pause.
    All,
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

/// Resolves the burn amount from exactly one of `amount` and `all`; both or
/// neither is a 400, like the CLI's clap conflict.
fn burn_amount(
    amount: Option<&str>,
    all: bool,
) -> Result<BurnAmount, (StatusCode, Json<ErrorResponse>)> {
    match (amount, all) {
        (Some(amount), false) => {
            let (_, raw) = positive_usdc(amount)?;
            Ok(BurnAmount::Exact(raw))
        }
        (None, true) => Ok(BurnAmount::All),
        (Some(_), true) | (None, false) => Err(ops_precondition_error(
            "specify exactly one of amount and all",
        )),
    }
}

/// The chains a CCTP burn from `from` leaves and lands on.
const fn cctp_route(from: CctpSourceChain) -> (Chain, Chain) {
    match from {
        CctpSourceChain::Ethereum => (Chain::Ethereum, Chain::Base),
        CctpSourceChain::Base => (Chain::Base, Chain::Ethereum),
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
/// `process-tx`: the routes sign with the bot's wallets, and so does the job
/// `transfer-usdc` enqueues. The recovery handle and the wallets exist before
/// the startup preflights (each chain's id, the inventory `OPERATOR_ROLE`)
/// have passed, and `health.is_ready()` gates on all of them.
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

    // A chain without a signer (Robinhood without its optional one) is
    // refused rather than signed with another chain's wallet, as in the CLI;
    // it is a gap in the bot's config, so it is a 500 like a missing wallet.
    let wallet = crate::conductor::chain_wallet(bot_wallets(state)?, chain).map_err(|error| {
        error!(%chain, ?error, "Capital route refused: the bot has no signer for the chain");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("The bot has no signer for {chain}"),
            }),
        )
    })?;

    Ok((trading, Arc::clone(wallet)))
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

/// Reads the token's decimals, scales the amount to the token's smallest unit
/// like the CLI's `float_to_u256`, and runs the operation through the chain's
/// `RaindexService`, built from the bot's signer like the startup revoke. The
/// reads run in the detached task too, so the whole sequence is one tracked
/// unit.
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
    let VaultTarget {
        chain,
        token,
        vault_id,
        amount,
    } = target;
    let route = operation.route();

    spawn_detached(&state.detached_tasks, route, token, async move {
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
                    .deposit::<OpenChainErrorRegistry>(
                        token,
                        RaindexVaultId(vault_id),
                        amount_raw,
                        decimals,
                    )
                    .await
            }
            VaultOperation::Withdraw | VaultOperation::WithdrawUsdc => {
                raindex
                    .withdraw(token, RaindexVaultId(vault_id), amount_raw, decimals)
                    .await
            }
        };
        let tx = submitted.map_err(|error| onchain_failure(route, chain, &error))?;
        info!(route, %chain, %token, %vault_id, %amount_raw, %tx, "Vault operation completed via API");

        Ok::<_, (StatusCode, Json<ErrorResponse>)>(VaultOutcome {
            decimals,
            amount_raw,
            tx,
        })
    })?
    .await?
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
            BurnAmount::Exact(U256::from(100_000_000_u64))
        );
        assert_eq!(burn_amount(None, true).unwrap(), BurnAmount::All);

        for (amount, all) in [(Some("100"), true), (None, false), (Some("0"), false)] {
            let Err((status, _)) = burn_amount(amount, all) else {
                panic!("amount {amount:?} with all {all} must be refused");
            };
            assert_eq!(status, StatusCode::BAD_REQUEST);
        }
    }

    #[test]
    fn cctp_request_reads_from_and_either_amount_or_all() {
        let request: CctpBridgeRequest =
            serde_json::from_value(serde_json::json!({"from": "base", "all": true})).unwrap();
        assert!(request.all);
        assert_eq!(request.amount, None);
        assert_eq!(cctp_route(request.from), (Chain::Base, Chain::Ethereum));

        let request: CctpBridgeRequest =
            serde_json::from_value(serde_json::json!({"from": "ethereum", "amount": "100"}))
                .unwrap();
        assert!(!request.all);
        assert_eq!(request.amount.as_deref(), Some("100"));
        assert_eq!(cctp_route(request.from), (Chain::Ethereum, Chain::Base));
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

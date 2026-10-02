//! The capital group of the ops write API: typed routes for the capital verbs
//! of `st0x-cli`, run inside the bot so they sign with its own wallets and
//! share their nonce state instead of building a second signer.
//!
//! No route waits on CCTP attestation, on USDC settlement, or on the
//! confirmations of the transaction it answers with, since the ops load
//! balancer times out first: `transfer-usdc` enqueues the transfer on the
//! bot's own worker and returns its id, and every route that sends a
//! transaction answers at its broadcast through `answer_from_detached` and
//! confirms it on the task. Only a prerequisite approve, when an allowance is
//! short, is awaited before that broadcast. The routes that
//! send transactions, and the
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
use futures_util::FutureExt as _;
use futures_util::future::BoxFuture;
use rain_math_float::Float;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use st0x_bridge::Bridge;
use st0x_bridge::corridor::UsdcCorridor;
use st0x_config::{HedgedChain, OnchainWalletCtx};
use st0x_event_sorcery::Store;
use st0x_evm::{Chain, Evm, IERC20, MinedTx, OpenChainErrorRegistry, PreparedTransaction, Wallet};
use st0x_finance::{HasZero, Positive, Usdc};
use st0x_float_serde::format_float_with_fallback;
use st0x_raindex::{Raindex, RaindexError, RaindexService, RaindexVaultId, RevokeOutcome};

use super::{
    ErrorResponse, OpsError, UsdcDriverPauseRequest, answer_from_detached, ops_command_error,
    ops_precondition_error, quiesce_usdc_driver, spawn_detached, usdc_resume_error_response,
};
use crate::AppState;
use crate::cctp_burn::{
    BotCctpBridge, BurnNotSuperseded, BurnReceiptFate, CctpBurnOperation, CctpBurnOperationCommand,
    CctpBurnOperationId, CctpBurnStatus, CctpSourceChain, NonceTakenBy, RequestedBurn,
    bot_cctp_bridge, burn_receipt_fate, record_burn_fate, settle_burn_from_receipt,
    signed_by_source_wallet, verify_burn_superseded,
};
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
    /// Chain of the served cash corridor to run on, as for
    /// `st0x-cli transfer-usdc --chain`; may be left out while the build
    /// serves one corridor.
    #[serde(default)]
    chain: Option<Chain>,
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
    Json(request): Json<CctpBridgeRequest>,
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
                    error = %crate::telemetry::scrub_secrets(&error.to_string()),
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
        error!(target: "operational_alert", alert = true, %operation_id, %burn_tx, ?direction, %failure, "A recorded CCTP burn could not be sent, and its receipt does not prove its outcome");
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
            error!(target: "operational_alert", alert = true, %operation_id, %burn_tx, nonce, ?error, "Cannot tell whether a signed CCTP burn was recorded; its nonce stays reserved and every later send from the source wallet waits behind it until a restart");
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
    let error = bridge.confirm_burn(direction, burn_tx, amount).await.err();
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
                %operation_id, %burn_tx, ?direction, %amount, ?error, ?read,
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
    Json(request): Json<CctpBurnSupersedeRequest>,
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

/// How long a vault route waits before it awaits its tx's confirmation again
/// after an inconclusive outcome (a receipt timeout, a drop report, or an RPC
/// failure). The graceful shutdown drain has its own timeout, so the wait
/// never blocks exit.
#[cfg(not(test))]
const VAULT_CONFIRM_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(30);
#[cfg(test)]
const VAULT_CONFIRM_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(50);

/// What a vault tx's confirmation attempt proved.
#[derive(Debug, PartialEq, Eq)]
enum VaultTxFate {
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

/// Decides a vault tx's fate after `confirm_tx` failed with `error`, from the
/// error and from `mined`, the tx's own canonical receipt as the node shows it
/// now. The receipt decides whenever it has `required_confirmations`, so a
/// revert whose replay cannot be decoded (no revert data, or pruned state)
/// still ends the wait.
fn vault_tx_fate(
    error: &RaindexError,
    mined: Option<&MinedTx>,
    required_confirmations: u64,
) -> VaultTxFate {
    if let Some(mined) = mined
        && mined.confirmations >= required_confirmations
    {
        return if mined.succeeded {
            VaultTxFate::Confirmed
        } else {
            VaultTxFate::Failed
        };
    }
    match error {
        RaindexError::Evm(evm) if evm.is_revert() => VaultTxFate::Failed,
        RaindexError::InsufficientVaultLiquidity { .. } => VaultTxFate::Failed,
        RaindexError::Evm(_)
        | RaindexError::Contract(_)
        | RaindexError::Float(_)
        | RaindexError::ZeroAmount
        | RaindexError::RpcTransport(_)
        | RaindexError::SolType(_)
        | RaindexError::ScanInconclusive { .. }
        | RaindexError::ScanAnomalousLog { .. }
        | RaindexError::MissingOperatorRole { .. } => VaultTxFate::Unknown,
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
            let wait = async {
                loop {
                    let error = match raindex.confirm_tx(tx).await {
                        Ok(()) => {
                            info!(
                                route, %chain, %token, %vault_id, %amount_raw, %tx,
                                "Vault operation confirmed via API"
                            );
                            return;
                        }
                        Err(error) => error,
                    };
                    // The tx's own receipt decides when the error does not:
                    // a revert whose replay cannot be decoded still mined.
                    let mined = raindex.mined_tx(tx).await.unwrap_or_else(|read_error| {
                        warn!(route, %chain, %tx, ?read_error, "Could not read the vault tx's receipt");
                        None
                    });
                    match vault_tx_fate(&error, mined.as_ref(), required_confirmations) {
                        VaultTxFate::Confirmed => {
                            info!(
                                route, %chain, %token, %vault_id, %amount_raw, %tx, ?error,
                                "Vault operation confirmed via API"
                            );
                            return;
                        }
                        VaultTxFate::Failed => {
                            error!(
                                route, %chain, %token, %vault_id, %amount_raw, %tx, ?error,
                                "Vault operation broadcast via API did not confirm"
                            );
                            return;
                        }
                        // A receipt timeout, a drop report, or an RPC failure
                        // proves nothing: the tx can still land, and releasing
                        // the lock would let a rerun send a second one.
                        VaultTxFate::Unknown => {
                            warn!(
                                route, %chain, %token, %vault_id, %amount_raw, %tx, ?error,
                                "Vault operation broadcast via API is not confirmed yet; keeping \
                                 its lock and waiting again"
                            );
                            tokio::time::sleep(VAULT_CONFIRM_RETRY_DELAY).await;
                        }
                    }
                }
            };
            // A panic would unwind through the guard and free the lock while
            // the tx's fate is unknown, so it is caught here, the guard is
            // kept until a restart, and the panic resumes for the watcher's
            // join failure log.
            match std::panic::AssertUnwindSafe(wait).catch_unwind().await {
                Ok(()) => drop(vault_guard),
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
        assert_eq!(vault_tx_fate(&dropped(), None, 3), VaultTxFate::Unknown);
    }

    /// The tx's own receipt at the required depth decides over the error,
    /// whichever way it went.
    #[test]
    fn a_receipt_at_the_required_depth_decides_the_fate() {
        assert_eq!(
            vault_tx_fate(&dropped(), Some(&mined(true, 3)), 3),
            VaultTxFate::Confirmed
        );
        assert_eq!(
            vault_tx_fate(&unreplayable(), Some(&mined(false, 3)), 3),
            VaultTxFate::Failed
        );
    }

    /// A receipt short of the required depth can still be reorged out, so
    /// only a decoded revert decides then.
    #[test]
    fn a_shallow_receipt_does_not_decide_the_fate() {
        assert_eq!(
            vault_tx_fate(&unreplayable(), Some(&mined(false, 2)), 3),
            VaultTxFate::Unknown
        );
        assert_eq!(
            vault_tx_fate(&dropped(), Some(&mined(true, 2)), 3),
            VaultTxFate::Unknown
        );
        assert_eq!(
            vault_tx_fate(
                &RaindexError::Evm(EvmError::Reverted {
                    tx_hash: TxHash::ZERO
                }),
                None,
                3
            ),
            VaultTxFate::Failed
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

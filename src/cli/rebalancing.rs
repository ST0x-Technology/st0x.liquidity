//! Transfer equity and USDC rebalancing CLI commands.

use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use anyhow::Context;
use sqlx::SqlitePool;
use std::future::Future;
use std::io::{self, Write};
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;
use uuid::Uuid;

use st0x_bridge::cctp::{CctpBridge, CctpCtx};
use st0x_event_sorcery::StoreBuilder;
use st0x_evm::{Evm, IERC20, OpenChainErrorRegistry, ReadOnlyEvm, USDC_BASE, USDC_ETHEREUM};
use st0x_execution::{
    AlpacaBrokerApi, AlpacaBrokerApiCtx, AlpacaBrokerApiMode, Executor, FractionalShares, Symbol,
    TimeInForce,
};
use st0x_finance::Usdc;
use st0x_raindex::{RaindexService, RaindexVaultId};
use st0x_wrapper::{Wrapper, WrapperService};

use super::{TransferDirection, TransferType};
use crate::alpaca_wallet::AlpacaWalletService;
use crate::equity_redemption::{EquityRedemption, EquityRedemptionCommand, RedemptionAggregateId};
use crate::rebalancing::equity::{CrossVenueEquityTransfer, EquityTransferServices};
use crate::rebalancing::to_wrapped_equities;
use crate::rebalancing::usdc::{CrossVenueCashTransfer, UsdcSettlementParams, UsdcTransferError};
use crate::tokenization::{
    AlpacaTokenizationService, TokenizationRequest, TokenizationRequestStatus, Tokenizer,
};
use crate::tokenized_equity_mint::{
    IssuerRequestId, TokenizedEquityMint, TokenizedEquityMintCommand,
};
use crate::usdc_rebalance::{
    RebalanceDirection, ReconcileReason, UsdcRebalance, UsdcRebalanceCommand, UsdcRebalanceId,
};
use crate::vault_lookup::{VaultLookup, VaultRegistryLookup};
use crate::vault_registry::VaultRegistry;
use st0x_config::{BrokerCtx, Ctx};

struct EquityTransferCliServices {
    transfer: CrossVenueEquityTransfer,
    wallet: Address,
}

/// Resolves the redemption wallet address from CLI flag or config.
///
/// CLI flag takes precedence. Falls back to `[tokenization]` config.
fn resolve_redemption_wallet(flag: Option<Address>, ctx: &Ctx) -> anyhow::Result<Address> {
    flag.map_or_else(|| ctx.redemption_wallet().map_err(Into::into), Ok)
}

async fn build_equity_transfer_services(
    redemption_wallet_flag: Option<Address>,
    ctx: &Ctx,
    pool: &SqlitePool,
) -> anyhow::Result<EquityTransferCliServices> {
    let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &ctx.broker else {
        anyhow::bail!("transfer-equity requires Alpaca Broker API configuration");
    };

    let redemption_wallet = resolve_redemption_wallet(redemption_wallet_flag, ctx)?;
    let wallet_ctx = ctx.wallet()?;
    let wallet = wallet_ctx.base_wallet().address();
    let base_caller = wallet_ctx.base_wallet().clone();

    let tokenization_service: Arc<dyn Tokenizer> = Arc::new(AlpacaTokenizationService::new(
        alpaca_auth.base_url().to_string(),
        alpaca_auth.account_id,
        alpaca_auth.api_key.clone(),
        alpaca_auth.api_secret.clone(),
        base_caller.clone(),
        Some(redemption_wallet),
    ));

    let (_vault_store, vault_registry_projection) =
        StoreBuilder::<VaultRegistry>::new(pool.clone())
            .build(())
            .await?;

    let wrapper: Arc<dyn Wrapper> = Arc::new(WrapperService::new(
        base_caller.clone(),
        to_wrapped_equities(&ctx.assets.equities.symbols),
    ));

    let vault_lookup: Arc<dyn VaultLookup> = Arc::new(VaultRegistryLookup::new(
        vault_registry_projection,
        ctx.evm.orderbook,
        wallet,
    ));

    let raindex = Arc::new(RaindexService::new(base_caller, ctx.evm.orderbook, wallet));

    let services = EquityTransferServices {
        raindex: raindex.clone(),
        vault_lookup: vault_lookup.clone(),
        tokenizer: tokenization_service.clone(),
        wrapper: wrapper.clone(),
    };

    let mint_store = StoreBuilder::<TokenizedEquityMint>::new(pool.clone())
        .build(services.clone())
        .await?;

    let redemption_store = StoreBuilder::<EquityRedemption>::new(pool.clone())
        .build(services.clone())
        .await?;

    let transfer = CrossVenueEquityTransfer::new(
        raindex,
        vault_lookup,
        tokenization_service.clone(),
        wrapper,
        wallet,
        mint_store,
        redemption_store,
    );

    Ok(EquityTransferCliServices { transfer, wallet })
}

pub(super) async fn transfer_equity_command<Writer: Write>(
    stdout: &mut Writer,
    direction: TransferDirection,
    symbol: &Symbol,
    quantity: FractionalShares,
    issuer_request_id: Option<Uuid>,
    redemption_wallet_flag: Option<Address>,
    ctx: &Ctx,
    pool: &SqlitePool,
) -> anyhow::Result<()> {
    let direction_str = match direction {
        TransferDirection::ToRaindex => "Alpaca → Raindex (mint)",
        TransferDirection::ToAlpaca => "Raindex → Alpaca (redeem)",
    };

    writeln!(stdout, "🔄 Transferring equity: {direction_str}")?;
    writeln!(stdout, "   Symbol: {symbol}")?;
    writeln!(stdout, "   Quantity: {quantity}")?;

    let cli_services = build_equity_transfer_services(redemption_wallet_flag, ctx, pool).await?;
    let equity_transfer = cli_services.transfer;

    match direction {
        TransferDirection::ToRaindex => {
            writeln!(stdout, "   Creating mint request...")?;
            writeln!(stdout, "   Receiving Wallet: {}", cli_services.wallet)?;

            let (issuer_request_id, fresh_mint) = issuer_request_id.map_or_else(
                || (IssuerRequestId::generate(), true),
                |uuid| (IssuerRequestId(uuid), false),
            );

            if fresh_mint {
                writeln!(stdout, "Equity mint issuer_request_id: {issuer_request_id}")?;
                writeln!(
                    stdout,
                    "   If this is interrupted, resume with:\n   \
                     transfer-equity --direction to-raindex --symbol {symbol} \
                     --quantity {quantity} --issuer-request-id {issuer_request_id}"
                )?;
                stdout.flush()?;
            }

            equity_transfer
                .resume_equity_to_market_making(&issuer_request_id, symbol, quantity)
                .await?;

            writeln!(stdout, "✅ Mint completed successfully")?;
        }

        TransferDirection::ToAlpaca => {
            writeln!(stdout, "   Sending tokens for redemption...")?;

            let aggregate_id = RedemptionAggregateId::generate();
            equity_transfer
                .resume_equity_to_hedging(&aggregate_id, symbol, quantity)
                .await?;

            writeln!(stdout, "✅ Redemption completed successfully")?;
        }
    }

    Ok(())
}

/// Per-retry delay for the manual CLI redrive loop when Circle's attestation is
/// not yet ready. Mirrors the apalis job's redrive cadence.
const CLI_ATTESTATION_REDRIVE_DELAY: Duration = Duration::from_secs(60);

/// Drives a manual USDC transfer to a terminal outcome, redriving on the same
/// retryable waits the apalis worker delayed-redrives -- Circle attestation
/// timeouts and the AlpacaToBase on-chain settlement gate -- so a single CLI
/// invocation cannot strand after the burn (or before it, during settlement
/// lag).
///
/// `resume` is re-invoked with the same `UsdcRebalanceId` on every attempt, so a
/// retry resumes the existing transfer instead of starting a second
/// fund-moving one. The retryable set mirrors the worker:
/// `AttestationTimedOut` plus the settlement-wait errors
/// (`WithdrawalTxUnderconfirmed`, `WalletUsdcInsufficient`,
/// `SettlementCheckTransient`). Any other error -- including
/// `AttestationRetryDeadlineElapsed` and a previously-failed aggregate -- is
/// terminal and returned to the caller.
async fn redrive_transfer_until_settled<Resume, Fut>(
    redrive_delay: Duration,
    mut resume: Resume,
) -> Result<(), UsdcTransferError>
where
    Resume: FnMut() -> Fut,
    Fut: Future<Output = Result<(), UsdcTransferError>>,
{
    loop {
        match resume().await {
            Ok(()) => return Ok(()),
            Err(UsdcTransferError::AttestationTimedOut { id }) => {
                warn!(
                    target: "rebalance",
                    %id,
                    ?redrive_delay,
                    "Circle attestation not ready for manual USDC transfer; retrying after delay"
                );
                tokio::time::sleep(redrive_delay).await;
            }
            Err(
                UsdcTransferError::WithdrawalTxUnderconfirmed { id, .. }
                | UsdcTransferError::WalletUsdcInsufficient { id, .. }
                | UsdcTransferError::SettlementCheckTransient { id, .. },
            ) => {
                warn!(
                    target: "rebalance",
                    %id,
                    ?redrive_delay,
                    "USDC transfer settlement not yet durable for manual transfer; \
                     retrying after delay"
                );
                tokio::time::sleep(redrive_delay).await;
            }
            Err(error) => return Err(error),
        }
    }
}

/// Starts a fresh manual USDC transfer. Surfaces the generated id up front so an
/// operator can resume it (via `resume-usdc-transfer`) if the process is
/// interrupted, then drives it to terminal with attestation-timeout redrive.
/// Whether a USDC transfer command may start a fresh transfer or must resume an
/// existing one. Drives the `None`-state policy in [`run_usdc_transfer`]: a
/// freshly generated id is expected to have no state (first run), but an
/// operator-supplied resume id that loads to `None` is a wrong/typoed id and
/// must be rejected -- never burned into a brand-new transfer.
#[derive(Clone, Copy)]
enum UsdcTransferStartMode {
    Fresh,
    Resume,
}

pub(super) async fn transfer_usdc_command<Writer: Write>(
    stdout: &mut Writer,
    direction: TransferDirection,
    amount: Usdc,
    ctx: &Ctx,
    pool: &SqlitePool,
) -> anyhow::Result<()> {
    let id = UsdcRebalanceId(Uuid::new_v4());
    let dir_flag = match direction {
        TransferDirection::ToRaindex => "to-raindex",
        TransferDirection::ToAlpaca => "to-alpaca",
    };
    writeln!(
        stdout,
        "USDC transfer id: {id}\n   If this is interrupted after the burn, resume with:\n   \
         resume-usdc-transfer --id {id} --direction {dir_flag} --amount {amount}"
    )?;
    // Flush the recovery id to durable output BEFORE the burn: the resume safety
    // story depends on the operator still having this id if the process is killed
    // after burning, and buffered/redirected stdout could otherwise lose it.
    stdout.flush()?;
    run_usdc_transfer(
        stdout,
        direction,
        amount,
        id,
        UsdcTransferStartMode::Fresh,
        ctx,
        pool,
    )
    .await
}

/// Resumes an interrupted manual USDC transfer by its id, driving it to terminal
/// with the same attestation-timeout redrive as a fresh transfer. Refuses to run
/// if the id has no persisted transfer (a wrong/typoed id would otherwise start a
/// brand-new burn) or if `--direction` disagrees with the persisted transfer. The
/// `--amount` is required for symmetry with the `transfer-usdc` recovery hint but
/// is not validated -- a resume uses the aggregate's persisted amount.
pub(super) async fn resume_usdc_transfer_command<Writer: Write>(
    stdout: &mut Writer,
    id: Uuid,
    direction: TransferDirection,
    amount: Usdc,
    ctx: &Ctx,
    pool: &SqlitePool,
) -> anyhow::Result<()> {
    let id = UsdcRebalanceId(id);
    writeln!(stdout, "Resuming USDC transfer id: {id}")?;
    run_usdc_transfer(
        stdout,
        direction,
        amount,
        id,
        UsdcTransferStartMode::Resume,
        ctx,
        pool,
    )
    .await
}

async fn run_usdc_transfer<Writer: Write>(
    stdout: &mut Writer,
    direction: TransferDirection,
    amount: Usdc,
    id: UsdcRebalanceId,
    mode: UsdcTransferStartMode,
    ctx: &Ctx,
    pool: &SqlitePool,
) -> anyhow::Result<()> {
    let dir = match direction {
        TransferDirection::ToRaindex => "Alpaca -> Raindex",
        TransferDirection::ToAlpaca => "Raindex -> Alpaca",
    };
    writeln!(stdout, "Transferring USDC: {dir}, Amount: {amount} USDC")?;

    let usdc_store = StoreBuilder::<UsdcRebalance>::new(pool.clone())
        .build(())
        .await?;

    // A resume must target an existing transfer, checked up front before any
    // broker/bridge setup. The manager's `resume_*` path treats a `None`-state id
    // as a first run and burns a brand-new transfer -- correct for a freshly
    // generated `transfer-usdc` id, but a fund-moving foot-gun for an
    // operator-supplied resume id that is mistyped or points at the wrong
    // database. Reject `None`, and reject a `--direction` that disagrees with the
    // persisted transfer (driving the opposite-direction resume path would
    // mis-drive the aggregate). The `--amount` is NOT validated: it stays in the
    // recovery command for symmetry with `transfer-usdc`, but a resume uses the
    // aggregate's persisted amount, and the persisted state amount is the
    // post-conversion/post-fee effective amount, not the original requested one.
    if matches!(mode, UsdcTransferStartMode::Resume) {
        let Some(state) = usdc_store.load(&id).await? else {
            anyhow::bail!(
                "resume-usdc-transfer: no transfer found for id {id}. Refusing to start a new \
                 burn -- check the id and that you are pointed at the right database."
            );
        };

        let expected_direction = match direction {
            TransferDirection::ToRaindex => RebalanceDirection::AlpacaToBase,
            TransferDirection::ToAlpaca => RebalanceDirection::BaseToAlpaca,
        };

        if state.direction() != expected_direction {
            anyhow::bail!(
                "resume-usdc-transfer: --direction does not match the persisted transfer for id \
                 {id} (persisted {:?}). Refusing to mis-drive the transfer.",
                state.direction()
            );
        }
    }

    let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &ctx.broker else {
        anyhow::bail!("transfer-usdc requires Alpaca Broker API configuration");
    };

    let wallet_ctx = ctx.wallet()?;

    let cash =
        ctx.assets.cash.as_ref().ok_or_else(|| {
            anyhow::anyhow!("assets.cash.vault_ids is required but not configured")
        })?;

    let usdc_vault_id =
        cash.vault_ids.first().copied().ok_or_else(|| {
            anyhow::anyhow!("assets.cash.vault_ids is required but not configured")
        })?;

    writeln!(stdout, "   Vault ID: {usdc_vault_id}")?;

    if cash.vault_ids.len() > 1 {
        writeln!(
            stdout,
            "   Warning: {} USDC vaults configured, using the first one",
            cash.vault_ids.len()
        )?;
    }
    let owner = wallet_ctx.base_wallet().address();

    let broker_mode = if alpaca_auth.is_sandbox() {
        AlpacaBrokerApiMode::Sandbox
    } else {
        AlpacaBrokerApiMode::Production
    };

    let broker_auth = AlpacaBrokerApiCtx {
        api_key: alpaca_auth.api_key.clone(),
        api_secret: alpaca_auth.api_secret.clone(),
        account_id: alpaca_auth.account_id,
        mode: Some(broker_mode),
        asset_cache_ttl: std::time::Duration::from_secs(3600),
        time_in_force: TimeInForce::default(),
        counter_trade_slippage_bps: alpaca_auth.counter_trade_slippage_bps,
    };

    let alpaca_broker = Arc::new(AlpacaBrokerApi::try_from_ctx(broker_auth.clone()).await?);

    let alpaca_wallet = Arc::new(AlpacaWalletService::new(
        broker_auth.base_url().to_string(),
        alpaca_auth.account_id,
        alpaca_auth.api_key.clone(),
        alpaca_auth.api_secret.clone(),
    ));

    let bridge = Arc::new(CctpBridge::try_from_ctx(CctpCtx {
        usdc_ethereum: USDC_ETHEREUM,
        usdc_base: USDC_BASE,
        ethereum_wallet: wallet_ctx.ethereum_wallet().clone(),
        base_wallet: wallet_ctx.base_wallet().clone(),
        #[cfg(feature = "test-support")]
        circle_api_base: st0x_bridge::cctp::CIRCLE_API_BASE.to_string(),
        #[cfg(feature = "test-support")]
        token_messenger: st0x_bridge::cctp::TOKEN_MESSENGER_V2,
        #[cfg(feature = "test-support")]
        message_transmitter: st0x_bridge::cctp::MESSAGE_TRANSMITTER_V2,
    })?);

    let vault_service = Arc::new(RaindexService::new(
        wallet_ctx.base_wallet().clone(),
        ctx.evm.orderbook,
        owner,
    ));

    let rebalancing_ctx = ctx.rebalancing_ctx()?;

    let rebalance_manager = CrossVenueCashTransfer::new(
        alpaca_broker,
        alpaca_wallet,
        bridge,
        vault_service,
        usdc_store,
        owner,
        RaindexVaultId(usdc_vault_id),
        &UsdcSettlementParams {
            attestation_retry_deadline: rebalancing_ctx.attestation_retry_deadline,
            required_confirmations: ctx.evm.required_confirmations,
            #[cfg(feature = "test-support")]
            circle_api_base: rebalancing_ctx.circle_api_base.clone(),
            #[cfg(feature = "test-support")]
            token_messenger: rebalancing_ctx.token_messenger,
            #[cfg(feature = "test-support")]
            message_transmitter: rebalancing_ctx.message_transmitter,
        },
    );

    writeln!(stdout, "   Transfer may take several minutes...")?;

    // Drive through `resume_*` (not `execute_*`): a fresh id with no prior state
    // takes the execute path inside `resume_*`, and an attestation timeout
    // re-enters from the persisted state on the same id -- so this single
    // invocation covers both the first run and every redrive without ever
    // minting a second `UsdcRebalanceId` against the already-burned funds.
    redrive_transfer_until_settled(CLI_ATTESTATION_REDRIVE_DELAY, || async {
        match direction {
            TransferDirection::ToRaindex => {
                rebalance_manager.resume_alpaca_to_base(&id, amount).await
            }
            TransferDirection::ToAlpaca => {
                rebalance_manager.resume_base_to_alpaca(&id, amount).await
            }
        }
    })
    .await?;

    let completion = match direction {
        TransferDirection::ToRaindex => "USDC transfer to Raindex completed successfully",
        TransferDirection::ToAlpaca => "USDC transfer to Alpaca completed successfully",
    };
    writeln!(stdout, "{completion}")?;

    Ok(())
}

/// Outcome of reloading a USDC rebalance immediately after sending `FailBridging`.
/// A pre-burn `BridgingFailed` is the intended guard-clearing result; anything
/// else means the transfer raced past us (a burn was recorded concurrently) or
/// landed in an unexpected state.
#[derive(Debug, PartialEq)]
enum FailBridgingOutcome {
    GuardCleared,
    ConcurrentBurn,
    Unexpected,
}

fn classify_fail_bridging_reload(state: Option<&UsdcRebalance>) -> FailBridgingOutcome {
    match state {
        Some(UsdcRebalance::BridgingFailed {
            burn_tx_hash: None, ..
        }) => FailBridgingOutcome::GuardCleared,
        Some(UsdcRebalance::BridgingFailed { .. }) => FailBridgingOutcome::ConcurrentBurn,
        None
        | Some(
            UsdcRebalance::Converting { .. }
            | UsdcRebalance::ConversionComplete { .. }
            | UsdcRebalance::ConversionFailed { .. }
            | UsdcRebalance::WithdrawalSubmitting { .. }
            | UsdcRebalance::Withdrawing { .. }
            | UsdcRebalance::WithdrawalFailed { .. }
            | UsdcRebalance::WithdrawalComplete { .. }
            | UsdcRebalance::BridgingSubmitting { .. }
            | UsdcRebalance::Bridging { .. }
            | UsdcRebalance::AwaitingAttestation { .. }
            | UsdcRebalance::Attested { .. }
            | UsdcRebalance::Bridged { .. }
            | UsdcRebalance::DepositInitiated { .. }
            | UsdcRebalance::DepositConfirmed { .. }
            | UsdcRebalance::DepositFailed { .. }
            | UsdcRebalance::Reconciled { .. },
        ) => FailBridgingOutcome::Unexpected,
    }
}

/// Drive a pre-burn `BridgingSubmitting` or `WithdrawalComplete` USDC rebalance
/// to the guard-clearing terminal `BridgingFailed { burn_tx_hash: None }`.
///
/// `WithdrawalComplete` is pre-CCTP-burn: no burn intent has been recorded.
/// However, the source withdrawal has already completed and the USDC is sitting
/// in the market-maker wallet awaiting bridging. After clearing the guard, the
/// operator must reconcile or handle those funds separately.
///
/// `BridgingSubmitting` records burn intent but does NOT guarantee no burn was
/// broadcast. A crash at this state may have broadcast a CCTP burn whose
/// `BridgingInitiated` event never persisted (see the crash-recovery invariant
/// in SPEC.md, "Crash-safe resume"). The operator MUST verify on-chain that no
/// recent CCTP burn left the market-maker wallet before using this command on a
/// `BridgingSubmitting` transfer; using it when a burn was broadcast strands
/// the already-burned funds.
///
/// Refuses all post-burn states (any state where `burn_tx_hash` or `cctp_nonce`
/// is recorded, or any post-burn leg: `Bridging`, `AwaitingAttestation`,
/// `Attested`, `Bridged`, and later deposit legs). The preflight is the
/// authoritative guard: `transition_fail_bridging` ACCEPTS post-burn `Bridging`/
/// `Attested` (emitting a guard-HOLDING `BridgingFailed`), so the aggregate does
/// NOT serve as a safety net here.
///
/// The command is CLI-direct (no running bot required). The live in-memory
/// `usdc_in_progress` guard is NOT cleared by this command. A bot restart is
/// required: `recover_usdc_guard` on startup skips
/// `BridgingFailed { burn_tx_hash: None }` (non-guard-holding) and does not
/// re-latch the guard.
pub(super) async fn fail_usdc_transfer_command<Writer: Write>(
    stdout: &mut Writer,
    id: Uuid,
    reason: &str,
    pool: &SqlitePool,
) -> anyhow::Result<()> {
    let id = UsdcRebalanceId(id);
    writeln!(stdout, "Failing pre-burn USDC transfer {id}")?;

    let usdc_store = StoreBuilder::<UsdcRebalance>::new(pool.clone())
        .build(())
        .await?;

    let Some(state) = usdc_store.load(&id).await? else {
        anyhow::bail!(
            "fail-usdc-transfer: no transfer found for id {id}. Refusing to act -- \
             check the id and that you are pointed at the right database."
        );
    };

    // AUTHORITATIVE post-burn guard: transition_fail_bridging accepts Bridging/
    // AwaitingAttestation/Attested and emits guard-HOLDING BridgingFailed.
    // This preflight is the only thing preventing an operator from accidentally
    // clearing the guard on a transfer where CCTP may have already broadcast.
    // Uses exhaustive match so new UsdcRebalance variants cause a compile error.
    // BaseToAlpaca ConversionFailed is post-deposit/post-burn (holds the guard
    // per holds_rebalance_guard()); AlpacaToBase ConversionFailed is pre-withdrawal.
    let is_post_burn = match &state {
        UsdcRebalance::Bridging { .. }
        | UsdcRebalance::AwaitingAttestation { .. }
        | UsdcRebalance::Attested { .. }
        | UsdcRebalance::Bridged { .. }
        | UsdcRebalance::DepositInitiated { .. }
        | UsdcRebalance::DepositConfirmed { .. }
        | UsdcRebalance::DepositFailed { .. }
        | UsdcRebalance::Reconciled { .. }
        | UsdcRebalance::ConversionFailed {
            direction: RebalanceDirection::BaseToAlpaca,
            ..
        } => true,
        UsdcRebalance::BridgingFailed { burn_tx_hash, .. } => burn_tx_hash.is_some(),
        UsdcRebalance::Converting { .. }
        | UsdcRebalance::ConversionComplete { .. }
        | UsdcRebalance::ConversionFailed {
            direction: RebalanceDirection::AlpacaToBase,
            ..
        }
        | UsdcRebalance::WithdrawalSubmitting { .. }
        | UsdcRebalance::Withdrawing { .. }
        | UsdcRebalance::WithdrawalFailed { .. }
        | UsdcRebalance::WithdrawalComplete { .. }
        | UsdcRebalance::BridgingSubmitting { .. } => false,
    };

    if is_post_burn {
        anyhow::bail!(
            "fail-usdc-transfer: transfer {id} is in a post-burn state. \
             A CCTP burn may have already been broadcast. Refusing to act -- \
             use reconcile-usdc-transfer for post-burn failures."
        );
    }

    // Early check: already in the pre-burn failed terminal. The guard is already
    // cleared (holds_rebalance_guard() returns false for burn_tx_hash: None) and
    // will not re-arm on restart. No action is needed and re-running would be a
    // no-op at best; return a clear error so the operator knows the state is good.
    if matches!(
        state,
        UsdcRebalance::BridgingFailed {
            burn_tx_hash: None,
            cctp_nonce: None,
            ..
        }
    ) {
        anyhow::bail!(
            "fail-usdc-transfer: transfer {id} is already in pre-burn BridgingFailed \
             (burn_tx_hash: None). The rebalancing guard is already in its cleared state \
             and will not re-arm on restart. No action needed."
        );
    }

    // Second gate: only BridgingSubmitting and WithdrawalComplete are accepted
    // by transition_fail_bridging. Pre-bridge states (Converting, Withdrawing,
    // etc.) return BridgingNotInitiated. Give the operator a clear error instead
    // of surfacing the internal aggregate error message.
    //
    // Exhaustive match: new UsdcRebalance variants must be placed explicitly so
    // the compiler flags unhandled variants at the gate boundary.
    //
    // Note: BridgingFailed reaches here only when burn_tx_hash is Some (the
    // early idempotency check above consumed burn_tx_hash:None, and is_post_burn
    // above rejected burn_tx_hash:Some via the post-burn gate). It is listed in
    // the bail group for exhaustiveness.
    match &state {
        UsdcRebalance::WithdrawalComplete { .. } | UsdcRebalance::BridgingSubmitting { .. } => {}
        UsdcRebalance::Converting { .. }
        | UsdcRebalance::ConversionComplete { .. }
        | UsdcRebalance::ConversionFailed { .. }
        | UsdcRebalance::Withdrawing { .. }
        | UsdcRebalance::WithdrawalSubmitting { .. }
        | UsdcRebalance::WithdrawalFailed { .. }
        | UsdcRebalance::Bridging { .. }
        | UsdcRebalance::AwaitingAttestation { .. }
        | UsdcRebalance::Attested { .. }
        | UsdcRebalance::Bridged { .. }
        | UsdcRebalance::BridgingFailed { .. }
        | UsdcRebalance::DepositInitiated { .. }
        | UsdcRebalance::DepositConfirmed { .. }
        | UsdcRebalance::DepositFailed { .. }
        | UsdcRebalance::Reconciled { .. } => {
            anyhow::bail!(
                "fail-usdc-transfer is only valid from BridgingSubmitting or WithdrawalComplete; \
                 transfer {id} is in {state:?} (a pre-bridging or post-bridging phase). \
                 Refusing to act."
            );
        }
    }

    usdc_store
        .send(
            &id,
            UsdcRebalanceCommand::FailBridging {
                reason: reason.to_string(),
            },
        )
        .await?;

    // Reload after send: if the bot raced us from BridgingSubmitting -> Bridging
    // between the preflight and the send, transition_fail_bridging would have
    // emitted a guard-HOLDING BridgingFailed with burn_tx_hash set. Check the
    // actual resulting state so we do not print a false "no burn" success message.
    //
    // A reload failure here must NOT report success: FailBridging committed, but
    // its outcome (guard cleared vs a concurrently-recorded burn) is unverified.
    // Returning an error forces the operator to re-check rather than treat the
    // guard-clear as confirmed; re-running the command is idempotent and yields
    // the accurate state.
    let reloaded = usdc_store.load(&id).await.with_context(|| {
        format!(
            "fail-usdc-transfer: FailBridging committed for transfer {id}, but re-reading to \
             verify the final state failed. Re-run the command to confirm whether the guard \
             cleared (pre-burn) or a burn was recorded concurrently before proceeding."
        )
    })?;
    match classify_fail_bridging_reload(reloaded.as_ref()) {
        FailBridgingOutcome::GuardCleared => {
            writeln!(
                stdout,
                "USDC transfer {id} transitioned to BridgingFailed (pre-burn, no burn \
                 recorded at transition time). The rebalancing guard will clear on the \
                 next bot restart."
            )?;
        }
        FailBridgingOutcome::ConcurrentBurn => {
            anyhow::bail!(
                "fail-usdc-transfer: a CCTP burn was recorded concurrently for transfer {id}. \
                 The guard was NOT cleared. Use reconcile-usdc-transfer for this post-burn \
                 failure. NOTE: reconcile-usdc-transfer must only be used after confirming \
                 on-chain that the CCTP burn did NOT complete (or the minted USDC was \
                 accounted for out-of-band); premature reconciliation on an in-flight bridge \
                 strands the arriving funds."
            );
        }
        FailBridgingOutcome::Unexpected => {
            anyhow::bail!(
                "fail-usdc-transfer: unexpected state after FailBridging for transfer {id}: \
                 {reloaded:?}"
            );
        }
    }

    Ok(())
}

/// Reconciles a USDC transfer stranded in a post-burn terminal failure to the
/// clearing terminal `Reconciled` state, releasing the in-progress guard.
///
/// The burned/minted USDC was handled out-of-band, so this resolves the
/// transfer (clearing the in-progress guard and reconciling source-venue
/// inflight via the reactor) rather than re-driving the failed leg. Builds a
/// standalone `UsdcRebalance` store and sends `ReconcileStuckRebalance`
/// directly -- no broker/bridge/vault is needed because reconciliation only
/// loads and sends. Rejects an unknown id (refusing to act on the wrong
/// transfer/database) and an aggregate that is not a guard-stranding post-burn
/// failure (the command itself rejects the latter, but the preflight gives a
/// clearer operator-facing error first).
pub(super) async fn reconcile_usdc_transfer_command<Writer: Write>(
    stdout: &mut Writer,
    id: Uuid,
    reason: ReconcileReason,
    pool: &SqlitePool,
) -> anyhow::Result<()> {
    let id = UsdcRebalanceId(id);
    writeln!(stdout, "Reconciling stuck USDC transfer id: {id}")?;

    let usdc_store = StoreBuilder::<UsdcRebalance>::new(pool.clone())
        .build(())
        .await?;

    let Some(state) = usdc_store.load(&id).await? else {
        anyhow::bail!(
            "reconcile-usdc-transfer: no transfer found for id {id}. Refusing to act -- check \
             the id and that you are pointed at the right database."
        );
    };

    // Authoritative gate is the aggregate command; this preflight mirrors its
    // accepted set (DepositFailed, post-burn BridgingFailed, BaseToAlpaca
    // ConversionFailed) only to give the operator a clearer error first.
    let is_post_burn_failure = matches!(
        state,
        UsdcRebalance::DepositFailed { .. }
            | UsdcRebalance::BridgingFailed {
                burn_tx_hash: Some(_),
                ..
            }
            | UsdcRebalance::BridgingFailed {
                cctp_nonce: Some(_),
                ..
            }
            | UsdcRebalance::ConversionFailed {
                direction: RebalanceDirection::BaseToAlpaca,
                ..
            }
    );

    if !is_post_burn_failure {
        anyhow::bail!(
            "reconcile-usdc-transfer: transfer {id} is in state {state:?}, not a post-burn \
             terminal failure that strands the in-progress guard (DepositFailed, post-burn \
             BridgingFailed, or a BaseToAlpaca ConversionFailed). Refusing to act."
        );
    }

    usdc_store
        .send(
            &id,
            UsdcRebalanceCommand::ReconcileStuckRebalance { reason },
        )
        .await?;

    writeln!(
        stdout,
        "Reconciled USDC transfer {id} (reason: {reason:?}); the in-progress guard will clear \
         on the next sweep tick (within transfer_timeout) and USDC rebalancing will resume \
         without a restart."
    )?;

    Ok(())
}

/// Isolated tokenization command - calls Alpaca tokenization API directly.
pub(super) async fn alpaca_tokenize_command<Writer: Write, Prov: Provider + Clone + 'static>(
    stdout: &mut Writer,
    symbol: Symbol,
    quantity: FractionalShares,
    token: Address,
    recipient: Option<Address>,
    ctx: &Ctx,
    provider: Prov,
) -> anyhow::Result<()> {
    writeln!(stdout, "🔄 Requesting tokenization via Alpaca API")?;
    writeln!(stdout, "   Symbol: {symbol}")?;
    writeln!(stdout, "   Quantity: {quantity}")?;
    writeln!(stdout, "   Token: {token}")?;

    let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &ctx.broker else {
        anyhow::bail!("alpaca-tokenize requires Alpaca Broker API configuration");
    };

    let wallet_ctx = ctx.wallet()?;

    let receiving_wallet = recipient.unwrap_or_else(|| wallet_ctx.base_wallet().address());
    writeln!(stdout, "   Receiving wallet: {receiving_wallet}")?;

    let read_evm = ReadOnlyEvm::new(provider.clone());
    let initial_balance: U256 = read_evm
        .call::<OpenChainErrorRegistry, _>(
            token,
            IERC20::balanceOfCall {
                account: receiving_wallet,
            },
        )
        .await?;

    writeln!(stdout, "   Initial balance: {initial_balance}")?;
    let expected_amount = quantity.to_u256_18_decimals()?;
    let expected_final = initial_balance
        .checked_add(expected_amount)
        .ok_or_else(|| {
            anyhow::anyhow!("balance overflow: {initial_balance} + {expected_amount}")
        })?;
    writeln!(stdout, "   Expected final balance: {expected_final}")?;

    let tokenization_service = AlpacaTokenizationService::new(
        alpaca_auth.base_url().to_string(),
        alpaca_auth.account_id,
        alpaca_auth.api_key.clone(),
        alpaca_auth.api_secret.clone(),
        wallet_ctx.base_wallet().clone(),
        None,
    );

    writeln!(stdout, "   Sending mint request to Alpaca...")?;

    let issuer_request_id = IssuerRequestId::generate();
    let request = tokenization_service
        .request_mint(
            symbol.clone(),
            quantity,
            receiving_wallet,
            issuer_request_id,
        )
        .await?;

    writeln!(stdout, "   Request ID: {}", request.id.0)?;
    writeln!(stdout, "   Status: {:?}", request.status)?;

    if request.status == TokenizationRequestStatus::Pending {
        writeln!(stdout, "   Polling Alpaca until completion...")?;

        let completed = tokenization_service
            .poll_mint_until_complete(&request.id)
            .await?;

        writeln!(stdout, "   Alpaca status: {:?}", completed.status)?;

        if let Some(tx_hash) = completed.tx_hash {
            writeln!(stdout, "   Alpaca tx hash: {tx_hash}")?;
        }

        if completed.status == TokenizationRequestStatus::Rejected {
            writeln!(stdout, "❌ Tokenization was rejected by Alpaca")?;
            return Ok(());
        }
    }

    writeln!(stdout, "   Polling for tokens to arrive on Base...")?;

    let poll_interval = std::time::Duration::from_secs(5);
    let max_attempts = 60; // 5 minutes max

    for attempt in 1..=max_attempts {
        let current_balance: U256 = read_evm
            .call::<OpenChainErrorRegistry, _>(
                token,
                IERC20::balanceOfCall {
                    account: receiving_wallet,
                },
            )
            .await?;

        if current_balance >= expected_final {
            writeln!(stdout, "   Final balance: {current_balance}")?;
            writeln!(stdout, "✅ Tokenization completed - tokens received!")?;
            return Ok(());
        }

        if attempt % 6 == 0 {
            writeln!(
                stdout,
                "   Still waiting... (attempt {attempt}/{max_attempts}, balance: {current_balance})",
            )?;
        }

        tokio::time::sleep(poll_interval).await;
    }

    let final_balance: U256 = read_evm
        .call::<OpenChainErrorRegistry, _>(
            token,
            IERC20::balanceOfCall {
                account: receiving_wallet,
            },
        )
        .await?;

    writeln!(stdout, "   Final balance: {final_balance}")?;
    writeln!(stdout, "⏳ Timed out waiting for tokens (may still arrive)")?;

    Ok(())
}

/// Isolated redemption command - calls Alpaca tokenization API directly.
pub(super) async fn alpaca_redeem_command<Writer: Write>(
    stdout: &mut Writer,
    symbol: Symbol,
    quantity: FractionalShares,
    token: Address,
    redemption_wallet_flag: Option<Address>,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    writeln!(stdout, "🔄 Requesting redemption via Alpaca API")?;
    writeln!(stdout, "   Symbol: {symbol}")?;
    writeln!(stdout, "   Quantity: {quantity}")?;
    writeln!(stdout, "   Token: {token}")?;

    let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &ctx.broker else {
        anyhow::bail!("alpaca-redeem requires Alpaca Broker API configuration");
    };

    let redemption_wallet = resolve_redemption_wallet(redemption_wallet_flag, ctx)?;
    let wallet_ctx = ctx.wallet()?;
    writeln!(stdout, "   Redemption wallet: {redemption_wallet}")?;

    let tokenization_service = AlpacaTokenizationService::new(
        alpaca_auth.base_url().to_string(),
        alpaca_auth.account_id,
        alpaca_auth.api_key.clone(),
        alpaca_auth.api_secret.clone(),
        wallet_ctx.base_wallet().clone(),
        Some(redemption_wallet),
    );

    let amount = quantity.to_u256_18_decimals()?;
    writeln!(stdout, "   Amount (wei): {amount}")?;

    writeln!(stdout, "   Sending tokens to redemption wallet...")?;

    let tx_hash = Tokenizer::send_for_redemption(&tokenization_service, token, amount).await?;

    writeln!(stdout, "   Transfer tx: {tx_hash}")?;
    writeln!(stdout, "   Waiting for Alpaca to detect transfer...")?;

    let request = Tokenizer::poll_for_redemption(&tokenization_service, &tx_hash).await?;

    writeln!(stdout, "   Request ID: {}", request.id.0)?;
    writeln!(stdout, "   Status: {:?}", request.status)?;

    if request.status == TokenizationRequestStatus::Pending {
        writeln!(stdout, "   Polling until completion...")?;

        let completed =
            Tokenizer::poll_redemption_until_complete(&tokenization_service, &request.id).await?;

        writeln!(stdout, "   Final status: {:?}", completed.status)?;

        match completed.status {
            TokenizationRequestStatus::Completed => {
                writeln!(stdout, "✅ Redemption completed successfully")?;
            }
            TokenizationRequestStatus::Rejected => {
                writeln!(stdout, "❌ Redemption was rejected")?;
            }
            TokenizationRequestStatus::Pending => {
                writeln!(stdout, "⏳ Redemption still pending (polling timed out)")?;
            }
        }
    }

    Ok(())
}

/// List all Alpaca tokenization requests.
pub(super) async fn alpaca_tokenization_requests_command<Writer: Write>(
    stdout: &mut Writer,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    writeln!(stdout, "📋 Listing Alpaca tokenization requests")?;

    let BrokerCtx::AlpacaBrokerApi(alpaca_auth) = &ctx.broker else {
        anyhow::bail!("alpaca-tokenization-requests requires Alpaca Broker API configuration");
    };

    let wallet_ctx = ctx.wallet()?;

    let tokenization_service = AlpacaTokenizationService::new(
        alpaca_auth.base_url().to_string(),
        alpaca_auth.account_id,
        alpaca_auth.api_key.clone(),
        alpaca_auth.api_secret.clone(),
        wallet_ctx.base_wallet().clone(),
        None,
    );

    let requests = tokenization_service.list_requests().await?;

    if requests.is_empty() {
        writeln!(stdout, "   No tokenization requests found")?;
        return Ok(());
    }

    writeln!(stdout, "   Found {} request(s):", requests.len())?;
    writeln!(stdout)?;

    for request in requests {
        format_tokenization_request(stdout, &request)?;
    }

    Ok(())
}

fn format_tokenization_request<Writer: Write>(
    stdout: &mut Writer,
    request: &TokenizationRequest,
) -> io::Result<()> {
    let type_str = request.r#type.map_or_else(
        || "unknown".to_string(),
        |transfer_type| transfer_type.to_string(),
    );

    let status_str = match request.status {
        TokenizationRequestStatus::Pending => "⏳ pending",
        TokenizationRequestStatus::Completed => "✅ completed",
        TokenizationRequestStatus::Rejected => "❌ rejected",
    };

    writeln!(stdout, "   ─────────────────────────────────────")?;
    writeln!(stdout, "   ID:       {}", request.id.0)?;
    writeln!(stdout, "   Type:     {type_str}")?;
    writeln!(stdout, "   Status:   {status_str}")?;
    writeln!(stdout, "   Symbol:   {}", request.underlying_symbol)?;
    writeln!(stdout, "   Quantity: {}", request.quantity)?;

    if let Some(ref wallet) = request.wallet {
        writeln!(stdout, "   Wallet:   {wallet}")?;
    }

    writeln!(stdout, "   Created:  {}", request.created_at)?;

    if let Some(ref tx_hash) = request.tx_hash {
        writeln!(stdout, "   Tx Hash:  {tx_hash}")?;
    }

    if let Some(ref issuer_id) = request.issuer_request_id {
        writeln!(stdout, "   Issuer ID: {}", issuer_id.0)?;
    }

    Ok(())
}

/// Manually fail a stuck mint or redemption transfer aggregate.
///
/// Loads the aggregate from the event store, determines its current state,
/// and sends the appropriate failure command. Rejects if the aggregate is
/// already in a terminal state.
pub(crate) async fn fail_transfer_command<W: Write>(
    stdout: &mut W,
    pool: &SqlitePool,
    transfer_type: TransferType,
    id: &str,
    reason: &str,
) -> anyhow::Result<()> {
    let services = EquityTransferServices::panicking();

    match transfer_type {
        TransferType::Mint => {
            let mint_id: IssuerRequestId = id
                .parse()
                .map_err(|error| anyhow::anyhow!("Invalid mint id {id:?}: {error}"))?;

            let entity = st0x_event_sorcery::load_entity::<TokenizedEquityMint>(pool, &mint_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("Mint aggregate not found: {id}"))?;

            use TokenizedEquityMint::*;
            use TokenizedEquityMintCommand::*;
            let command = match entity {
                MintAccepted { .. } => FailAcceptance {
                    reason: reason.to_string(),
                },
                TokensReceived { .. } | WrapSubmitted { .. } => FailWrapping {
                    reason: reason.to_string(),
                },
                TokensWrapped { .. } | VaultDepositSubmitted { .. } => FailRaindexDeposit {
                    reason: reason.to_string(),
                },
                MintRequested { .. } => {
                    anyhow::bail!("Mint {id} is at MintRequested -- cannot fail before acceptance");
                }
                DepositedIntoRaindex { .. } => {
                    anyhow::bail!("Mint {id} already completed (DepositedIntoRaindex)");
                }
                Failed { .. } => {
                    anyhow::bail!("Mint {id} already failed");
                }
            };

            st0x_event_sorcery::send_command::<TokenizedEquityMint>(
                pool, &mint_id, command, services,
            )
            .await?;

            writeln!(stdout, "Mint {id} marked as failed")?;
        }

        TransferType::Redemption => {
            let redemption_id: RedemptionAggregateId = id
                .parse()
                .map_err(|error| anyhow::anyhow!("Invalid redemption ID: {error}"))?;

            let entity = st0x_event_sorcery::load_entity::<EquityRedemption>(pool, &redemption_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("Redemption aggregate not found: {id}"))?;

            use EquityRedemption::*;
            match entity {
                VaultWithdrawPending { .. }
                | VaultWithdrawSubmitted { .. }
                | WithdrawnFromRaindex { .. }
                | UnwrapPending { .. }
                | UnwrapSubmitted { .. }
                | TokensUnwrapped { .. }
                | SendPending { .. } => {}
                TokensSent { .. } | Pending { .. } => {
                    anyhow::bail!(
                        "Redemption {id} is past the transfer stage -- \
                         use FailDetection or RejectRedemption instead"
                    );
                }
                Completed { .. } => {
                    anyhow::bail!("Redemption {id} already completed");
                }
                Failed { .. } => {
                    anyhow::bail!("Redemption {id} already failed");
                }
            }

            st0x_event_sorcery::send_command::<EquityRedemption>(
                pool,
                &redemption_id,
                EquityRedemptionCommand::FailTransfer {
                    reason: reason.to_string(),
                },
                services,
            )
            .await?;

            writeln!(stdout, "Redemption {id} marked as failed")?;
        }
    }

    Ok(())
}

/// Re-checks a stuck transfer by calling the running bot's
/// `/transfers/recheck` endpoint. Recovery must run in the bot process so the
/// recovery event dispatches through the reactor-wired store (correcting the
/// live inventory view) and shares the bot's resume lock. Requires the bot to
/// be running and serving its REST API on the configured `server_port`.
pub(crate) async fn recheck_transfer_command<W: Write>(
    stdout: &mut W,
    transfer_type: TransferType,
    id: &str,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    let kind = match transfer_type {
        TransferType::Mint => "equity_mint",
        TransferType::Redemption => "equity_redemption",
    };

    let url = format!(
        "http://127.0.0.1:{}/transfers/recheck/{kind}/{id}",
        ctx.server_port
    );
    writeln!(stdout, "Re-checking {transfer_type:?} {id} via {url}")?;

    // Bound the request so the CLI cannot hang indefinitely if the local bot
    // stalls after accepting the connection.
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;
    let response = client.post(&url).send().await?;
    let status = response.status();
    let body = response.text().await?;

    if !status.is_success() {
        anyhow::bail!("recheck-transfer failed ({status}): {body}");
    }

    // The endpoint returns `{"outcome":"<snake_case>"}`; surface the outcome
    // name (the operator-facing value documented in docs/cli-ops.md) rather
    // than the raw JSON envelope, falling back to the body if it can't be parsed.
    let outcome = serde_json::from_str::<serde_json::Value>(&body)
        .ok()
        .and_then(|value| {
            value
                .get("outcome")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or(body);
    writeln!(stdout, "recheck-transfer outcome: {outcome}")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, address, b256};
    use rain_math_float::Float;
    use url::Url;
    use uuid::uuid;

    use st0x_execution::{
        AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode, ClientOrderId, TimeInForce,
    };
    use st0x_finance::Usdc;
    use st0x_float_macro::float;

    use super::*;
    use crate::inventory::ImbalanceThreshold;
    use crate::test_utils::setup_test_db;
    use crate::usdc_rebalance::{ReconcileReason, TransferRef, UsdcRebalanceCommand};
    use st0x_config::EvmCtx;
    use st0x_config::ExecutionThreshold;
    use st0x_config::RebalancingCtx;
    use st0x_config::create_test_issuance_ctx;
    use st0x_config::{
        AssetsConfig, CashAssetConfig, EquitiesConfig, LogLevel, OperationMode, TradingMode,
    };

    /// RAI-835: the manual redrive loop must re-invoke `resume` on the same id
    /// after an attestation timeout and drive through to success -- so a single
    /// CLI invocation cannot strand a burned transfer.
    #[tokio::test]
    async fn redrive_loop_retries_attestation_timeout_then_succeeds() {
        let calls = std::cell::Cell::new(0u32);

        let result = redrive_transfer_until_settled(Duration::from_millis(0), || {
            let attempt = calls.get() + 1;
            calls.set(attempt);
            async move {
                if attempt == 1 {
                    Err(UsdcTransferError::AttestationTimedOut {
                        id: UsdcRebalanceId(Uuid::from_u128(7)),
                    })
                } else {
                    Ok(())
                }
            }
        })
        .await;

        result.expect("redrive must succeed once the attestation arrives");
        assert_eq!(
            calls.get(),
            2,
            "the loop must retry exactly once after the timeout, then succeed",
        );
    }

    /// The manual redrive loop must also retry the on-chain settlement-wait
    /// errors the apalis worker delayed-redrives -- otherwise a manual transfer
    /// would exit on normal settlement lag instead of continuing to completion.
    #[tokio::test]
    async fn redrive_loop_retries_settlement_wait_then_succeeds() {
        let calls = std::cell::Cell::new(0u32);

        let result = redrive_transfer_until_settled(Duration::from_millis(0), || {
            let attempt = calls.get() + 1;
            calls.set(attempt);
            async move {
                if attempt == 1 {
                    Err(UsdcTransferError::WithdrawalTxUnderconfirmed {
                        id: UsdcRebalanceId(Uuid::from_u128(9)),
                        tx: b256!(
                            "0x0000000000000000000000000000000000000000000000000000000000000001"
                        ),
                        required: 3,
                        actual: 1,
                    })
                } else {
                    Ok(())
                }
            }
        })
        .await;

        result.expect("redrive must succeed once the withdrawal tx settles");
        assert_eq!(
            calls.get(),
            2,
            "the loop must retry exactly once after the settlement wait, then succeed",
        );
    }

    /// RAI-835: a non-timeout terminal outcome (deadline elapsed) must end the
    /// loop immediately, not spin forever.
    #[tokio::test]
    async fn redrive_loop_returns_terminal_error_without_looping() {
        let calls = std::cell::Cell::new(0u32);

        let result = redrive_transfer_until_settled(Duration::from_millis(0), || {
            calls.set(calls.get() + 1);
            async move {
                Err(UsdcTransferError::AttestationRetryDeadlineElapsed {
                    id: UsdcRebalanceId(Uuid::from_u128(7)),
                })
            }
        })
        .await;

        let error = result.unwrap_err();
        assert!(
            matches!(
                error,
                UsdcTransferError::AttestationRetryDeadlineElapsed { .. }
            ),
            "a deadline-elapsed outcome must surface, not be retried; got {error:?}",
        );
        assert_eq!(calls.get(), 1, "a terminal error must not be retried");
    }

    fn create_ctx_without_rebalancing() -> Ctx {
        Ctx {
            database_url: ":memory:".to_string(),
            log_level: LogLevel::Debug,
            log_dir: None,
            server_port: 8080,
            board_port: 8081,
            evm: EvmCtx {
                rpc_url: Url::parse("http://localhost:8545").unwrap(),
                orderbook: address!("0x1234567890123456789012345678901234567890"),
                deployment_block: 1,
                required_confirmations: 0,
            },
            order_polling_interval: 15,
            order_polling_max_jitter: 5,
            position_check_interval: 60,
            inventory_poll_interval: 60,
            order_fill_poll_interval: 5,
            apalis_finished_job_cleanup_interval_secs: 3600,
            broker: BrokerCtx::DryRun,
            telemetry: None,
            alerts: None,
            trading_mode: TradingMode::Standalone,
            order_owner: Address::ZERO,
            wallet: None,
            wallet_meta: None,
            execution_threshold: ExecutionThreshold::whole_share(),
            assets: AssetsConfig {
                equities: EquitiesConfig::default(),
                cash: None,
            },
            travel_rule: None,
            rest_api: None,
            issuance: create_test_issuance_ctx(),
            redemption_wallet: None,
        }
    }

    fn create_alpaca_ctx_without_rebalancing() -> Ctx {
        let mut ctx = create_ctx_without_rebalancing();
        ctx.broker = BrokerCtx::AlpacaBrokerApi(AlpacaBrokerApiCtx {
            api_key: "test-key".to_string(),
            api_secret: "test-secret".to_string(),
            account_id: AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b")),
            mode: Some(AlpacaBrokerApiMode::Sandbox),
            asset_cache_ttl: std::time::Duration::from_secs(3600),
            time_in_force: TimeInForce::default(),
            counter_trade_slippage_bps: st0x_execution::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
        });
        ctx
    }

    fn create_alpaca_ctx_with_rebalancing(cash: Option<CashAssetConfig>) -> Ctx {
        let alpaca_broker_auth = AlpacaBrokerApiCtx {
            api_key: "test-key".to_string(),
            api_secret: "test-secret".to_string(),
            account_id: AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b")),
            mode: Some(AlpacaBrokerApiMode::Sandbox),
            asset_cache_ttl: std::time::Duration::from_secs(3600),
            time_in_force: TimeInForce::default(),
            counter_trade_slippage_bps: st0x_execution::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
        };

        Ctx {
            database_url: ":memory:".to_string(),
            log_level: LogLevel::Debug,
            log_dir: None,
            server_port: 8080,
            board_port: 8081,
            evm: EvmCtx {
                rpc_url: Url::parse("http://localhost:8545").unwrap(),
                orderbook: address!("0x1234567890123456789012345678901234567890"),
                deployment_block: 1,
                required_confirmations: 0,
            },
            order_polling_interval: 15,
            order_polling_max_jitter: 5,
            position_check_interval: 60,
            inventory_poll_interval: 60,
            order_fill_poll_interval: 5,
            apalis_finished_job_cleanup_interval_secs: 3600,
            broker: BrokerCtx::AlpacaBrokerApi(alpaca_broker_auth),
            telemetry: None,
            alerts: None,
            trading_mode: TradingMode::Rebalancing(Box::new(
                RebalancingCtx::stub()
                    .equity(ImbalanceThreshold {
                        target: float!(0.5),
                        deviation: float!(0.1),
                    })
                    .usdc(ImbalanceThreshold {
                        target: float!(0.5),
                        deviation: float!(0.1),
                    })
                    .call(),
            )),
            order_owner: Address::ZERO,
            wallet: Some(st0x_config::OnchainWalletCtx::stub()),
            wallet_meta: None,
            execution_threshold: ExecutionThreshold::whole_share(),
            assets: AssetsConfig {
                equities: EquitiesConfig::default(),
                cash,
            },
            travel_rule: None,
            rest_api: None,
            issuance: create_test_issuance_ctx(),
            redemption_wallet: Some(Address::ZERO),
        }
    }

    #[tokio::test]
    async fn test_transfer_equity_requires_alpaca_broker() {
        let ctx = create_ctx_without_rebalancing();
        let pool = setup_test_db().await;
        let symbol = Symbol::new("AAPL").unwrap();
        let quantity = FractionalShares::new(Float::parse("10.5".to_string()).unwrap());

        let mut stdout = Vec::new();
        let result = transfer_equity_command(
            &mut stdout,
            TransferDirection::ToRaindex,
            &symbol,
            quantity,
            None,
            None,
            &ctx,
            &pool,
        )
        .await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("requires Alpaca Broker API configuration"),
            "Expected Alpaca Broker API error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_transfer_equity_requires_tokenization_config() {
        let ctx = create_alpaca_ctx_without_rebalancing();
        let pool = setup_test_db().await;
        let symbol = Symbol::new("AAPL").unwrap();
        let quantity = FractionalShares::new(Float::parse("10.5".to_string()).unwrap());

        let mut stdout = Vec::new();
        let result = transfer_equity_command(
            &mut stdout,
            TransferDirection::ToRaindex,
            &symbol,
            quantity,
            None,
            None,
            &ctx,
            &pool,
        )
        .await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("requires [tokenization]"),
            "Expected tokenization config error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_transfer_usdc_requires_alpaca_broker() {
        let ctx = create_ctx_without_rebalancing();
        let pool = setup_test_db().await;
        let amount = Usdc::new(Float::parse("100".to_string()).unwrap());

        let mut stdout = Vec::new();
        let result = transfer_usdc_command(
            &mut stdout,
            TransferDirection::ToRaindex,
            amount,
            &ctx,
            &pool,
        )
        .await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("requires Alpaca Broker API configuration"),
            "Expected Alpaca Broker API error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn resume_usdc_transfer_rejects_unknown_id_without_burning() {
        // The core safety contract of `resume-usdc-transfer`: an id that has no
        // persisted transfer (a typo, or the wrong database) MUST be rejected up
        // front rather than falling through to the manager's `None` -> fresh-burn
        // path. The existence check runs before any broker/bridge setup, so a bare
        // ctx and an empty pool reach it directly.
        let ctx = create_ctx_without_rebalancing();
        let pool = setup_test_db().await;
        let amount = Usdc::new(Float::parse("100".to_string()).unwrap());
        let unknown_id = Uuid::from_u128(0xDEAD_BEEF);

        let mut stdout = Vec::new();
        let result = resume_usdc_transfer_command(
            &mut stdout,
            unknown_id,
            TransferDirection::ToRaindex,
            amount,
            &ctx,
            &pool,
        )
        .await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("no transfer found for id"),
            "resume of an unknown id must refuse, not start a new burn; got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn reconcile_usdc_transfer_rejects_unknown_id() {
        let pool = setup_test_db().await;
        let unknown_id = Uuid::from_u128(0xFEED_FACE);

        let mut stdout = Vec::new();
        let result = reconcile_usdc_transfer_command(
            &mut stdout,
            unknown_id,
            ReconcileReason::FundsMovedManually,
            &pool,
        )
        .await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("no transfer found for id"),
            "reconcile of an unknown id must refuse; got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn reconcile_usdc_transfer_rejects_in_progress_aggregate() {
        // An Initiated (in-progress, pre-burn) aggregate is not a guard-stranding
        // post-burn failure, so the preflight must reject it before sending the
        // reconcile command.
        let pool = setup_test_db().await;
        let id = Uuid::from_u128(7777);

        let store = StoreBuilder::<UsdcRebalance>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        store
            .send(
                &UsdcRebalanceId(id),
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc::new(Float::parse("100".to_string()).unwrap()),
                    withdrawal: TransferRef::OnchainTx(b256!(
                        "0x00000000000000000000000000000000000000000000000000000000000000b1"
                    )),
                },
            )
            .await
            .unwrap();

        let mut stdout = Vec::new();
        let result = reconcile_usdc_transfer_command(
            &mut stdout,
            id,
            ReconcileReason::FundsMovedManually,
            &pool,
        )
        .await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("not a post-burn terminal failure that strands the in-progress guard"),
            "reconcile of an in-progress aggregate must refuse; got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn resume_usdc_transfer_rejects_direction_mismatch() {
        // Seed an AlpacaToBase transfer, then resume with the opposite direction
        // (`--direction to-alpaca` => BaseToAlpaca). The guard must reject it
        // rather than driving the aggregate through the wrong-direction resume
        // path. The check runs before broker setup, so a bare ctx reaches it.
        let ctx = create_ctx_without_rebalancing();
        let pool = setup_test_db().await;
        let amount = Usdc::new(Float::parse("100".to_string()).unwrap());
        let id = Uuid::from_u128(99);

        let store = StoreBuilder::<UsdcRebalance>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        store
            .send(
                &UsdcRebalanceId(id),
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount,
                    withdrawal: TransferRef::OnchainTx(b256!(
                        "0x00000000000000000000000000000000000000000000000000000000000000a1"
                    )),
                },
            )
            .await
            .unwrap();

        let mut stdout = Vec::new();
        let result = resume_usdc_transfer_command(
            &mut stdout,
            id,
            TransferDirection::ToAlpaca,
            amount,
            &ctx,
            &pool,
        )
        .await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("does not match the persisted transfer"),
            "resume with the wrong direction must be rejected, not mis-drive; got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn resume_usdc_transfer_accepts_amount_differing_from_persisted() {
        // Regression guard: a resume with the CORRECT direction but a `--amount`
        // different from the persisted state amount must NOT be rejected. The
        // persisted amount is the post-slippage/post-fee effective amount, not the
        // original requested one the operator types, so validating against it
        // would wrongly reject legitimate resumes (the iter2 bug). The preflight
        // guard must let it through -- here it then fails at broker setup, proving
        // the guard accepted it.
        let ctx = create_ctx_without_rebalancing();
        let pool = setup_test_db().await;
        let seeded_amount = Usdc::new(Float::parse("100".to_string()).unwrap());
        let different_amount = Usdc::new(Float::parse("250".to_string()).unwrap());
        let id = Uuid::from_u128(123);

        let store = StoreBuilder::<UsdcRebalance>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        store
            .send(
                &UsdcRebalanceId(id),
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: seeded_amount,
                    withdrawal: TransferRef::OnchainTx(b256!(
                        "0x00000000000000000000000000000000000000000000000000000000000000a2"
                    )),
                },
            )
            .await
            .unwrap();

        // ToRaindex maps to AlpacaToBase -- the correct direction for the seed.
        let mut stdout = Vec::new();
        let result = resume_usdc_transfer_command(
            &mut stdout,
            id,
            TransferDirection::ToRaindex,
            different_amount,
            &ctx,
            &pool,
        )
        .await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            !err_msg.contains("does not match"),
            "a differing --amount must pass the guard (resume uses persisted amount); got: {err_msg}"
        );
        assert!(
            err_msg.contains("requires Alpaca Broker API configuration"),
            "past the guard, the bare ctx must fail at broker setup; got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_transfer_usdc_requires_wallet_config() {
        let mut ctx = create_alpaca_ctx_without_rebalancing();
        ctx.assets.cash = Some(CashAssetConfig {
            vault_ids: vec![b256!(
                "0x00000000000000000000000000000000000000000000000000000000000000ab"
            )],
            rebalancing: OperationMode::Enabled,
            operational_limit: None,
            reserved: None,
        });
        let pool = setup_test_db().await;
        let amount = Usdc::new(Float::parse("100".to_string()).unwrap());

        let mut stdout = Vec::new();
        let result = transfer_usdc_command(
            &mut stdout,
            TransferDirection::ToRaindex,
            amount,
            &ctx,
            &pool,
        )
        .await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("configured [wallet] section"),
            "Expected wallet config error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_transfer_usdc_writes_direction_to_stdout() {
        let ctx = create_alpaca_ctx_without_rebalancing();
        let pool = setup_test_db().await;
        let amount = Usdc::new(Float::parse("100".to_string()).unwrap());

        let mut stdout = Vec::new();
        transfer_usdc_command(
            &mut stdout,
            TransferDirection::ToRaindex,
            amount,
            &ctx,
            &pool,
        )
        .await
        .unwrap_err();

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("Alpaca -> Raindex"),
            "Expected direction in output, got: {output}"
        );
    }

    #[test]
    fn cli_broker_mode_sandbox_when_sandbox_auth() {
        let alpaca_auth = AlpacaBrokerApiCtx {
            api_key: "test-key".to_string(),
            api_secret: "test-secret".to_string(),
            account_id: AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b")),
            mode: Some(AlpacaBrokerApiMode::Sandbox),
            asset_cache_ttl: std::time::Duration::from_secs(3600),
            time_in_force: TimeInForce::default(),
            counter_trade_slippage_bps: st0x_execution::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
        };

        let broker_mode = if alpaca_auth.is_sandbox() {
            AlpacaBrokerApiMode::Sandbox
        } else {
            AlpacaBrokerApiMode::Production
        };

        assert_eq!(
            broker_mode,
            AlpacaBrokerApiMode::Sandbox,
            "Sandbox auth should yield Sandbox broker mode"
        );
    }

    #[test]
    fn cli_broker_mode_production_when_production_auth() {
        let alpaca_auth = AlpacaBrokerApiCtx {
            api_key: "test-key".to_string(),
            api_secret: "test-secret".to_string(),
            account_id: AlpacaAccountId::new(uuid!("904837e3-3b76-47ec-b432-046db621571b")),
            mode: Some(AlpacaBrokerApiMode::Production),
            asset_cache_ttl: std::time::Duration::from_secs(3600),
            time_in_force: TimeInForce::default(),
            counter_trade_slippage_bps: st0x_execution::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
        };

        let broker_mode = if alpaca_auth.is_sandbox() {
            AlpacaBrokerApiMode::Sandbox
        } else {
            AlpacaBrokerApiMode::Production
        };

        assert_eq!(
            broker_mode,
            AlpacaBrokerApiMode::Production,
            "Production auth should yield Production broker mode"
        );
    }

    #[tokio::test]
    async fn test_transfer_usdc_requires_vault_id_when_cash_is_none() {
        let ctx = create_alpaca_ctx_with_rebalancing(None);
        let pool = setup_test_db().await;
        let amount = Usdc::new(Float::parse("100".to_string()).unwrap());

        let mut stdout = Vec::new();
        let result = transfer_usdc_command(
            &mut stdout,
            TransferDirection::ToRaindex,
            amount,
            &ctx,
            &pool,
        )
        .await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("assets.cash.vault_ids is required"),
            "Expected vault_id error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_transfer_usdc_requires_vault_id_when_vault_id_is_none() {
        let ctx = create_alpaca_ctx_with_rebalancing(Some(CashAssetConfig {
            vault_ids: Vec::new(),
            rebalancing: OperationMode::Enabled,
            operational_limit: None,
            reserved: None,
        }));
        let pool = setup_test_db().await;
        let amount = Usdc::new(Float::parse("100".to_string()).unwrap());

        let mut stdout = Vec::new();
        let result = transfer_usdc_command(
            &mut stdout,
            TransferDirection::ToRaindex,
            amount,
            &ctx,
            &pool,
        )
        .await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("assets.cash.vault_ids is required"),
            "Expected vault_id error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_transfer_usdc_writes_vault_id_to_stdout() {
        let vault_id = b256!("0x00000000000000000000000000000000000000000000000000000000000000ab");
        let ctx = create_alpaca_ctx_with_rebalancing(Some(CashAssetConfig {
            vault_ids: vec![vault_id],
            rebalancing: OperationMode::Enabled,
            operational_limit: None,
            reserved: None,
        }));
        let pool = setup_test_db().await;
        let amount = Usdc::new(Float::parse("100".to_string()).unwrap());

        let mut stdout = Vec::new();
        // The vault lookup succeeds, then the command fails later when it reaches
        // the stubbed service setup.
        transfer_usdc_command(
            &mut stdout,
            TransferDirection::ToRaindex,
            amount,
            &ctx,
            &pool,
        )
        .await
        .unwrap_err();

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("Vault ID:"),
            "Expected vault ID in output, got: {output}"
        );
    }

    #[test]
    fn resolve_redemption_wallet_flag_takes_precedence() {
        let mut ctx = create_alpaca_ctx_without_rebalancing();
        let config_wallet = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let flag_wallet = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        ctx.redemption_wallet = Some(config_wallet);

        let result = resolve_redemption_wallet(Some(flag_wallet), &ctx).unwrap();
        assert_eq!(result, flag_wallet);
    }

    #[test]
    fn resolve_redemption_wallet_falls_back_to_config() {
        let mut ctx = create_alpaca_ctx_without_rebalancing();
        let config_wallet = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        ctx.redemption_wallet = Some(config_wallet);

        let result = resolve_redemption_wallet(None, &ctx).unwrap();
        assert_eq!(result, config_wallet);
    }

    #[test]
    fn resolve_redemption_wallet_errors_when_missing() {
        let ctx = create_alpaca_ctx_without_rebalancing();
        assert_eq!(ctx.redemption_wallet, None);

        let result = resolve_redemption_wallet(None, &ctx);
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("requires [tokenization]"),
            "Expected tokenization config error, got: {err_msg}"
        );
    }

    // Seeds a UsdcRebalance aggregate to WithdrawalComplete state via:
    // Initiate (Uninitialized->Withdrawing) -> ConfirmWithdrawal (->WithdrawalComplete)
    async fn seed_to_withdrawal_complete(
        store: &st0x_event_sorcery::Store<UsdcRebalance>,
        id: Uuid,
    ) {
        let usdc_id = UsdcRebalanceId(id);
        let amount = Usdc::new(Float::parse("100".to_string()).unwrap());
        store
            .send(
                &usdc_id,
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount,
                    withdrawal: TransferRef::OnchainTx(b256!(
                        "0x0000000000000000000000000000000000000000000000000000000000000001"
                    )),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &usdc_id,
                UsdcRebalanceCommand::ConfirmWithdrawal {
                    withdrawal_tx: None,
                },
            )
            .await
            .unwrap();
    }

    // Seeds from WithdrawalComplete to BridgingSubmitting via BeginBridging.
    async fn seed_to_bridging_submitting(
        store: &st0x_event_sorcery::Store<UsdcRebalance>,
        id: Uuid,
    ) {
        seed_to_withdrawal_complete(store, id).await;
        store
            .send(
                &UsdcRebalanceId(id),
                UsdcRebalanceCommand::BeginBridging {
                    from_block: 1,
                    burn_amount: None,
                },
            )
            .await
            .unwrap();
    }

    // Seeds from BridgingSubmitting to Bridging via InitiateBridging.
    async fn seed_to_bridging(store: &st0x_event_sorcery::Store<UsdcRebalance>, id: Uuid) {
        seed_to_bridging_submitting(store, id).await;
        store
            .send(
                &UsdcRebalanceId(id),
                UsdcRebalanceCommand::InitiateBridging {
                    burn_tx: b256!(
                        "0x000000000000000000000000000000000000000000000000000000000000dead"
                    ),
                },
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn fail_usdc_transfer_rejects_unknown_id() {
        let pool = setup_test_db().await;
        let unknown_id = Uuid::from_u128(0xCAFE_BABE);

        let mut stdout = Vec::new();
        let result = fail_usdc_transfer_command(&mut stdout, unknown_id, "reason", &pool).await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("no transfer found for id"),
            "unknown id must be rejected; got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn fail_usdc_transfer_rejects_post_burn_bridging_state() {
        // Bridging state has burn_tx_hash recorded; the preflight must block it
        // because transition_fail_bridging would emit a guard-HOLDING BridgingFailed.
        let pool = setup_test_db().await;
        let id = Uuid::from_u128(0xBEEF_0001);

        let store = StoreBuilder::<UsdcRebalance>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        seed_to_bridging(&store, id).await;

        let mut stdout = Vec::new();
        let result = fail_usdc_transfer_command(&mut stdout, id, "should fail", &pool).await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("post-burn"),
            "Bridging state must be rejected as post-burn; got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn fail_usdc_transfer_rejects_post_burn_awaiting_attestation() {
        // AwaitingAttestation is also accepted by transition_fail_bridging and
        // emits guard-HOLDING BridgingFailed -- the preflight must block it.
        let pool = setup_test_db().await;
        let id = Uuid::from_u128(0xBEEF_0002);

        let store = StoreBuilder::<UsdcRebalance>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        seed_to_bridging(&store, id).await;
        store
            .send(
                &UsdcRebalanceId(id),
                UsdcRebalanceCommand::TimeoutAttestation {
                    retry_deadline_at: chrono::Utc::now() + chrono::Duration::hours(1),
                },
            )
            .await
            .unwrap();

        let mut stdout = Vec::new();
        let result = fail_usdc_transfer_command(&mut stdout, id, "should fail", &pool).await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("post-burn"),
            "AwaitingAttestation state must be rejected as post-burn; got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn fail_usdc_transfer_rejects_attested_state() {
        // Attested has both burn_tx_hash and cctp_nonce recorded; the preflight must
        // block it because transition_fail_bridging would emit a guard-HOLDING
        // BridgingFailed.
        let pool = setup_test_db().await;
        let id = Uuid::from_u128(0xBEEF_0002_000B);

        let store = StoreBuilder::<UsdcRebalance>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        seed_to_bridging(&store, id).await;
        store
            .send(
                &UsdcRebalanceId(id),
                UsdcRebalanceCommand::ReceiveAttestation {
                    attestation: vec![0xAB],
                    cctp_nonce: b256!(
                        "0x000000000000000000000000000000000000000000000000000000000000CAFE"
                    ),
                    message: vec![0xCD],
                    mint_scan_from_block: 1,
                },
            )
            .await
            .unwrap();

        let mut stdout = Vec::new();
        let result = fail_usdc_transfer_command(&mut stdout, id, "should fail", &pool).await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("post-burn"),
            "Attested state must be rejected as post-burn; got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn fail_usdc_transfer_rejects_pre_bridge_converting_state() {
        // Converting is before the bridging phase; transition_fail_bridging would
        // return BridgingNotInitiated. The second gate must give a clear error.
        let pool = setup_test_db().await;
        let id = Uuid::from_u128(0xBEEF_0003);

        let store = StoreBuilder::<UsdcRebalance>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        store
            .send(
                &UsdcRebalanceId(id),
                UsdcRebalanceCommand::InitiateConversion {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(Float::parse("100".to_string()).unwrap()),
                    order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
                },
            )
            .await
            .unwrap();

        let mut stdout = Vec::new();
        let result = fail_usdc_transfer_command(&mut stdout, id, "should fail", &pool).await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("only valid from BridgingSubmitting or WithdrawalComplete"),
            "Converting state must be rejected as not in bridging phase; got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn fail_usdc_transfer_rejects_already_terminal_pre_burn() {
        // A pre-burn BridgingFailed (burn_tx_hash: None) is already in the
        // guard-cleared terminal. The command must return a clear error telling
        // the operator no action is needed, rather than a confusing "not in
        // bridging phase" message.
        let pool = setup_test_db().await;
        let id = Uuid::from_u128(0xBEEF_0004);

        let store = StoreBuilder::<UsdcRebalance>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        seed_to_bridging_submitting(&store, id).await;
        store
            .send(
                &UsdcRebalanceId(id),
                UsdcRebalanceCommand::FailBridging {
                    reason: "seeded".to_string(),
                },
            )
            .await
            .unwrap();

        // Now in BridgingFailed { burn_tx_hash: None } -- the early check fires.
        let mut stdout = Vec::new();
        let result = fail_usdc_transfer_command(&mut stdout, id, "re-fail", &pool).await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("already in pre-burn BridgingFailed"),
            "Already-terminal pre-burn BridgingFailed must return a clear 'already failed' \
             error; got: {err_msg}"
        );
        assert!(
            err_msg.contains("No action needed"),
            "Error must tell the operator no action is needed; got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn fail_usdc_transfer_succeeds_on_bridging_submitting() {
        let pool = setup_test_db().await;
        let id = Uuid::from_u128(0xBEEF_0005);

        let store = StoreBuilder::<UsdcRebalance>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        seed_to_bridging_submitting(&store, id).await;

        let mut stdout = Vec::new();
        let result = fail_usdc_transfer_command(&mut stdout, id, "manual fail", &pool).await;
        result.expect("fail_usdc_transfer must succeed from BridgingSubmitting");

        let state = store.load(&UsdcRebalanceId(id)).await.unwrap().unwrap();
        let UsdcRebalance::BridgingFailed {
            burn_tx_hash,
            cctp_nonce,
            reason,
            ..
        } = state
        else {
            panic!("Expected BridgingFailed state, got: {state:?}");
        };
        assert_eq!(
            burn_tx_hash, None,
            "pre-burn BridgingFailed must have no burn_tx_hash"
        );
        assert_eq!(
            cctp_nonce, None,
            "pre-burn BridgingFailed must have no cctp_nonce"
        );
        assert_eq!(reason, "manual fail", "reason must be persisted in event");

        let reloaded = store.load(&UsdcRebalanceId(id)).await.unwrap().unwrap();
        assert!(
            !reloaded.holds_rebalance_guard(),
            "BridgingFailed with burn_tx_hash: None must NOT hold the rebalancing guard"
        );

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("restart"),
            "success message must mention 'restart'; got: {output}"
        );
    }

    #[tokio::test]
    async fn fail_usdc_transfer_succeeds_on_withdrawal_complete() {
        let pool = setup_test_db().await;
        let id = Uuid::from_u128(0xBEEF_0006);

        let store = StoreBuilder::<UsdcRebalance>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        seed_to_withdrawal_complete(&store, id).await;

        let mut stdout = Vec::new();
        let result = fail_usdc_transfer_command(&mut stdout, id, "wc-distinct-reason", &pool).await;
        result.expect("fail_usdc_transfer must succeed from WithdrawalComplete");

        let state = store.load(&UsdcRebalanceId(id)).await.unwrap().unwrap();
        let UsdcRebalance::BridgingFailed {
            burn_tx_hash,
            cctp_nonce,
            reason,
            ..
        } = state
        else {
            panic!("Expected BridgingFailed state, got: {state:?}");
        };
        assert_eq!(
            burn_tx_hash, None,
            "pre-burn BridgingFailed must have no burn_tx_hash"
        );
        assert_eq!(
            cctp_nonce, None,
            "pre-burn BridgingFailed must have no cctp_nonce"
        );
        assert_eq!(
            reason, "wc-distinct-reason",
            "reason must be persisted in event"
        );

        let reloaded = store.load(&UsdcRebalanceId(id)).await.unwrap().unwrap();
        assert!(
            !reloaded.holds_rebalance_guard(),
            "BridgingFailed with burn_tx_hash: None must NOT hold the rebalancing guard"
        );

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("restart"),
            "success message must mention 'restart'; got: {output}"
        );
    }

    // Seeds from Bridging to DepositFailed via:
    // ReceiveAttestation -> ConfirmBridging -> InitiateDeposit -> FailDeposit
    async fn seed_to_deposit_failed(store: &st0x_event_sorcery::Store<UsdcRebalance>, id: Uuid) {
        let usdc_id = UsdcRebalanceId(id);
        seed_to_bridging(store, id).await;
        store
            .send(
                &usdc_id,
                UsdcRebalanceCommand::ReceiveAttestation {
                    attestation: vec![0xAA],
                    cctp_nonce: b256!(
                        "0x000000000000000000000000000000000000000000000000000000000000CAFE"
                    ),
                    message: vec![0xBB],
                    mint_scan_from_block: 1,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &usdc_id,
                UsdcRebalanceCommand::ConfirmBridging {
                    mint_tx: b256!(
                        "0x000000000000000000000000000000000000000000000000000000000000BEEF"
                    ),
                    amount_received: Usdc::new(Float::parse("100".to_string()).unwrap()),
                    fee_collected: Usdc::new(Float::parse("0".to_string()).unwrap()),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &usdc_id,
                UsdcRebalanceCommand::InitiateDeposit {
                    deposit: TransferRef::OnchainTx(b256!(
                        "0x000000000000000000000000000000000000000000000000000000000000DEED"
                    )),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &usdc_id,
                UsdcRebalanceCommand::FailDeposit {
                    reason: "seeded deposit failure".to_string(),
                },
            )
            .await
            .unwrap();
    }

    // Seeds from Bridging to Bridged via:
    // ReceiveAttestation -> ConfirmBridging
    async fn seed_to_bridged(store: &st0x_event_sorcery::Store<UsdcRebalance>, id: Uuid) {
        let usdc_id = UsdcRebalanceId(id);
        seed_to_bridging(store, id).await;
        store
            .send(
                &usdc_id,
                UsdcRebalanceCommand::ReceiveAttestation {
                    attestation: vec![0xAA],
                    cctp_nonce: b256!(
                        "0x000000000000000000000000000000000000000000000000000000000000CAFE"
                    ),
                    message: vec![0xBB],
                    mint_scan_from_block: 1,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &usdc_id,
                UsdcRebalanceCommand::ConfirmBridging {
                    mint_tx: b256!(
                        "0x000000000000000000000000000000000000000000000000000000000000BEEF"
                    ),
                    amount_received: Usdc::new(Float::parse("100".to_string()).unwrap()),
                    fee_collected: Usdc::new(Float::parse("0".to_string()).unwrap()),
                },
            )
            .await
            .unwrap();
    }

    // Seeds from Bridged to DepositInitiated via InitiateDeposit.
    async fn seed_to_deposit_initiated(store: &st0x_event_sorcery::Store<UsdcRebalance>, id: Uuid) {
        seed_to_bridged(store, id).await;
        store
            .send(
                &UsdcRebalanceId(id),
                UsdcRebalanceCommand::InitiateDeposit {
                    deposit: TransferRef::OnchainTx(b256!(
                        "0x000000000000000000000000000000000000000000000000000000000000DEED"
                    )),
                },
            )
            .await
            .unwrap();
    }

    // Seeds from DepositInitiated to DepositConfirmed via ConfirmDeposit.
    async fn seed_to_deposit_confirmed(store: &st0x_event_sorcery::Store<UsdcRebalance>, id: Uuid) {
        seed_to_deposit_initiated(store, id).await;
        store
            .send(&UsdcRebalanceId(id), UsdcRebalanceCommand::ConfirmDeposit)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn fail_usdc_transfer_rejects_bridged_state() {
        // Bridged has burn_tx_hash set (post-burn); the preflight must block it.
        let pool = setup_test_db().await;
        let id = Uuid::from_u128(0xBEEF_000B);

        let store = StoreBuilder::<UsdcRebalance>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        seed_to_bridged(&store, id).await;

        let mut stdout = Vec::new();
        let result = fail_usdc_transfer_command(&mut stdout, id, "should fail", &pool).await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("post-burn"),
            "Bridged state must be rejected as post-burn; got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn fail_usdc_transfer_rejects_deposit_initiated_state() {
        // DepositInitiated is post-burn (burn_tx_hash recorded); the preflight must block it.
        let pool = setup_test_db().await;
        let id = Uuid::from_u128(0xBEEF_000C);

        let store = StoreBuilder::<UsdcRebalance>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        seed_to_deposit_initiated(&store, id).await;

        let mut stdout = Vec::new();
        let result = fail_usdc_transfer_command(&mut stdout, id, "should fail", &pool).await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("post-burn"),
            "DepositInitiated state must be rejected as post-burn; got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn fail_usdc_transfer_rejects_deposit_confirmed_state() {
        // DepositConfirmed is post-burn (burn_tx_hash recorded); the preflight must block it.
        let pool = setup_test_db().await;
        let id = Uuid::from_u128(0xBEEF_000D);

        let store = StoreBuilder::<UsdcRebalance>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        seed_to_deposit_confirmed(&store, id).await;

        let mut stdout = Vec::new();
        let result = fail_usdc_transfer_command(&mut stdout, id, "should fail", &pool).await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("post-burn"),
            "DepositConfirmed state must be rejected as post-burn; got: {err_msg}"
        );
    }

    // Seeds from DepositConfirmed (BaseToAlpaca) to ConversionFailed via:
    // InitiatePostDepositConversion -> FailConversion
    async fn seed_to_conversion_failed_base_to_alpaca(
        store: &st0x_event_sorcery::Store<UsdcRebalance>,
        id: Uuid,
    ) {
        seed_to_deposit_confirmed(store, id).await;
        store
            .send(
                &UsdcRebalanceId(id),
                UsdcRebalanceCommand::InitiatePostDepositConversion {
                    order_id: ClientOrderId::from_uuid(Uuid::from_u128(0xC01D_0000)),
                    amount: Usdc::new(Float::parse("100".to_string()).unwrap()),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &UsdcRebalanceId(id),
                UsdcRebalanceCommand::FailConversion {
                    reason: "seeded conversion failure".to_string(),
                },
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn fail_usdc_transfer_rejects_conversion_failed_base_to_alpaca() {
        // ConversionFailed{BaseToAlpaca} is post-deposit/post-burn and holds the
        // rebalancing guard (per holds_rebalance_guard()). The is_post_burn gate
        // must reject it so the operator is routed to reconcile-usdc-transfer.
        let pool = setup_test_db().await;
        let id = Uuid::from_u128(0xBEEF_000E);

        let store = StoreBuilder::<UsdcRebalance>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        seed_to_conversion_failed_base_to_alpaca(&store, id).await;

        let mut stdout = Vec::new();
        let result = fail_usdc_transfer_command(&mut stdout, id, "should fail", &pool).await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("post-burn"),
            "ConversionFailed{{BaseToAlpaca}} must be rejected as post-burn; got: {err_msg}"
        );
    }

    /// The classifier correctly identifies each of the three post-send reload
    /// outcomes: pre-burn `BridgingFailed` (guard cleared), post-burn
    /// `BridgingFailed` (concurrent burn raced us), and any other state
    /// (unexpected).
    ///
    /// Inputs are built by driving real aggregate state through the store so the
    /// test remains honest if the enum shape changes.
    #[tokio::test]
    async fn classify_fail_bridging_reload_distinguishes_pre_and_post_burn() {
        let pool = setup_test_db().await;
        let store = StoreBuilder::<UsdcRebalance>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        // Case 1: pre-burn BridgingFailed (GuardCleared).
        // Seed to BridgingSubmitting, send FailBridging, load.
        let id_pre_burn = Uuid::from_u128(0xC1A5_0001);
        seed_to_bridging_submitting(&store, id_pre_burn).await;
        store
            .send(
                &UsdcRebalanceId(id_pre_burn),
                UsdcRebalanceCommand::FailBridging {
                    reason: "pre-burn test".to_string(),
                },
            )
            .await
            .unwrap();
        let pre_burn_state = store.load(&UsdcRebalanceId(id_pre_burn)).await.unwrap();
        assert_eq!(
            classify_fail_bridging_reload(pre_burn_state.as_ref()),
            FailBridgingOutcome::GuardCleared,
            "pre-burn BridgingFailed must classify as GuardCleared"
        );

        // Case 2: post-burn BridgingFailed (ConcurrentBurn).
        // Seed to Bridging (burn_tx_hash recorded), send FailBridging, load.
        let id_post_burn = Uuid::from_u128(0xC1A5_0002);
        seed_to_bridging(&store, id_post_burn).await;
        store
            .send(
                &UsdcRebalanceId(id_post_burn),
                UsdcRebalanceCommand::FailBridging {
                    reason: "post-burn test".to_string(),
                },
            )
            .await
            .unwrap();
        let post_burn_state = store.load(&UsdcRebalanceId(id_post_burn)).await.unwrap();
        assert_eq!(
            classify_fail_bridging_reload(post_burn_state.as_ref()),
            FailBridgingOutcome::ConcurrentBurn,
            "post-burn BridgingFailed must classify as ConcurrentBurn"
        );

        // Case 3: a non-BridgingFailed state (Unexpected).
        // Load a seeded BridgingSubmitting without sending FailBridging.
        let id_unexpected = Uuid::from_u128(0xC1A5_0003);
        seed_to_bridging_submitting(&store, id_unexpected).await;
        let unexpected_state = store.load(&UsdcRebalanceId(id_unexpected)).await.unwrap();
        assert_eq!(
            classify_fail_bridging_reload(unexpected_state.as_ref()),
            FailBridgingOutcome::Unexpected,
            "BridgingSubmitting (not BridgingFailed) must classify as Unexpected"
        );

        // Case 4: None (Unexpected).
        assert_eq!(
            classify_fail_bridging_reload(None),
            FailBridgingOutcome::Unexpected,
            "None state must classify as Unexpected"
        );
    }

    /// Preflight rejects `BridgingFailed { burn_tx_hash: Some }` (post-burn)
    /// directly at the `is_post_burn` guard, before `store.send` is called.
    #[tokio::test]
    async fn fail_usdc_transfer_rejects_post_burn_bridging_failed() {
        let pool = setup_test_db().await;
        let id = Uuid::from_u128(0xBEEF_0008);

        let store = StoreBuilder::<UsdcRebalance>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        // Seed to Bridging (burn_tx_hash recorded), then FailBridging to reach
        // BridgingFailed { burn_tx_hash: Some }.
        seed_to_bridging(&store, id).await;
        store
            .send(
                &UsdcRebalanceId(id),
                UsdcRebalanceCommand::FailBridging {
                    reason: "seeded post-burn fail".to_string(),
                },
            )
            .await
            .unwrap();

        let mut stdout = Vec::new();
        let result = fail_usdc_transfer_command(&mut stdout, id, "should fail", &pool).await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("post-burn"),
            "BridgingFailed with burn_tx_hash: Some must be rejected as post-burn; got: {err_msg}"
        );
    }

    /// Preflight rejects `DepositFailed` (post-burn terminal) at the
    /// `is_post_burn` guard.
    #[tokio::test]
    async fn fail_usdc_transfer_rejects_deposit_failed() {
        let pool = setup_test_db().await;
        let id = Uuid::from_u128(0xBEEF_0009);

        let store = StoreBuilder::<UsdcRebalance>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        seed_to_deposit_failed(&store, id).await;

        let mut stdout = Vec::new();
        let result = fail_usdc_transfer_command(&mut stdout, id, "should fail", &pool).await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("post-burn"),
            "DepositFailed state must be rejected as post-burn; got: {err_msg}"
        );
    }

    /// Preflight rejects `Reconciled` (post-burn terminal) at the
    /// `is_post_burn` guard.
    #[tokio::test]
    async fn fail_usdc_transfer_rejects_reconciled() {
        let pool = setup_test_db().await;
        let id = Uuid::from_u128(0xBEEF_000A);

        let store = StoreBuilder::<UsdcRebalance>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        // Seed to DepositFailed then reconcile to Reconciled.
        seed_to_deposit_failed(&store, id).await;
        store
            .send(
                &UsdcRebalanceId(id),
                UsdcRebalanceCommand::ReconcileStuckRebalance {
                    reason: ReconcileReason::FundsMovedManually,
                },
            )
            .await
            .unwrap();

        let mut stdout = Vec::new();
        let result = fail_usdc_transfer_command(&mut stdout, id, "should fail", &pool).await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("post-burn"),
            "Reconciled state must be rejected as post-burn; got: {err_msg}"
        );
    }

    // Seeds a UsdcRebalance aggregate to ConversionFailed (AlpacaToBase direction) via:
    // InitiateConversion(AlpacaToBase) -> FailConversion
    async fn seed_to_conversion_failed_alpaca_to_base(
        store: &st0x_event_sorcery::Store<UsdcRebalance>,
        id: Uuid,
    ) {
        store
            .send(
                &UsdcRebalanceId(id),
                UsdcRebalanceCommand::InitiateConversion {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(Float::parse("100".to_string()).unwrap()),
                    order_id: ClientOrderId::from_uuid(Uuid::from_u128(0xC01D_0001)),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &UsdcRebalanceId(id),
                UsdcRebalanceCommand::FailConversion {
                    reason: "seeded AlpacaToBase conversion failure".to_string(),
                },
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn fail_usdc_transfer_rejects_conversion_failed_alpaca_to_base() {
        // ConversionFailed{AlpacaToBase} is pre-withdrawal (pre-burn): is_post_burn
        // returns false, so the second gate must reject it with the "only valid from
        // BridgingSubmitting or WithdrawalComplete" message.
        let pool = setup_test_db().await;
        let id = Uuid::from_u128(0xBEEF_000F);

        let store = StoreBuilder::<UsdcRebalance>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        seed_to_conversion_failed_alpaca_to_base(&store, id).await;

        let mut stdout = Vec::new();
        let result = fail_usdc_transfer_command(&mut stdout, id, "should fail", &pool).await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("only valid from BridgingSubmitting or WithdrawalComplete"),
            "ConversionFailed{{AlpacaToBase}} must be rejected as not in bridging phase; \
             got: {err_msg}"
        );
    }

    // Seeds a UsdcRebalance aggregate to WithdrawalFailed (BaseToAlpaca direction) via:
    // Initiate(BaseToAlpaca) -> FailWithdrawal
    async fn seed_to_withdrawal_failed(store: &st0x_event_sorcery::Store<UsdcRebalance>, id: Uuid) {
        let usdc_id = UsdcRebalanceId(id);
        store
            .send(
                &usdc_id,
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc::new(Float::parse("100".to_string()).unwrap()),
                    withdrawal: TransferRef::OnchainTx(b256!(
                        "0x0000000000000000000000000000000000000000000000000000000000000002"
                    )),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &usdc_id,
                UsdcRebalanceCommand::FailWithdrawal {
                    reason: "seeded withdrawal failure".to_string(),
                },
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn fail_usdc_transfer_rejects_withdrawal_failed() {
        // WithdrawalFailed is pre-burn; is_post_burn returns false, so the second
        // gate must reject it with the "only valid from BridgingSubmitting or
        // WithdrawalComplete" message.
        let pool = setup_test_db().await;
        let id = Uuid::from_u128(0xBEEF_0010);

        let store = StoreBuilder::<UsdcRebalance>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        seed_to_withdrawal_failed(&store, id).await;

        let mut stdout = Vec::new();
        let result = fail_usdc_transfer_command(&mut stdout, id, "should fail", &pool).await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("only valid from BridgingSubmitting or WithdrawalComplete"),
            "WithdrawalFailed state must be rejected as not in bridging phase; got: {err_msg}"
        );
    }
}

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
use st0x_event_sorcery::{AggregateError, StoreBuilder};
use st0x_evm::{Evm, IERC20, OpenChainErrorRegistry, ReadOnlyEvm, USDC_BASE, USDC_ETHEREUM};
use st0x_execution::{
    AlpacaBrokerApi, AlpacaBrokerApiCtx, AlpacaBrokerApiMode, AlpacaWalletService, Executor,
    FractionalShares, Symbol, TimeInForce,
};
use st0x_finance::Usdc;
use st0x_issuance_client::IssuanceClient;
use st0x_raindex::{RaindexService, RaindexVaultId};
use st0x_tokenization::{
    AlpacaTokenizationService, IssuerRequestId, TokenizationRequest, TokenizationRequestStatus,
    Tokenizer,
};
use st0x_wrapper::{Wrapper, WrapperService};

use super::{TransferDirection, TransferType};
use crate::api::ResumeResponse;
use crate::equity_redemption::{
    DetectionFailure, EquityRedemption, EquityRedemptionCommand, RedemptionAggregateId,
};
use crate::rebalancing::equity::CrossVenueEquityTransfer;
use crate::rebalancing::to_wrapped_equities;
use crate::rebalancing::trigger::freeze::FreezeStatusReader;
use crate::rebalancing::usdc::{CrossVenueCashTransfer, UsdcSettlementParams, UsdcTransferError};
use crate::telemetry::TelemetrySender;
use crate::telemetry::broker::InstrumentedAlpacaBroker;
use crate::tokenized_equity_mint::{TokenizedEquityMint, TokenizedEquityMintCommand};
use crate::usdc_rebalance::{
    RebalanceDirection, ReconcileReason, UsdcRebalance, UsdcRebalanceCommand, UsdcRebalanceId,
};
use crate::vault_lookup::{VaultLookup, VaultRegistryLookup};
use crate::vault_registry::{VaultRegistry, VaultRegistryId};
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
        VaultRegistryId {
            orderbook: ctx.evm.orderbook,
            owner: ctx.vault_owner(),
        },
    ));

    let raindex = Arc::new(RaindexService::new(
        base_caller,
        crate::onchain::raindex_contracts(&ctx.evm),
        wallet,
    ));

    let mint_store = StoreBuilder::<TokenizedEquityMint>::new(pool.clone())
        .build(())
        .await?;

    let redemption_store = StoreBuilder::<EquityRedemption>::new(pool.clone())
        .build(())
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

/// Operator's stance toward the dividend freeze gate on the manual
/// redemption-side commands (`transfer-equity --direction to-alpaca`,
/// `alpaca-redeem`). Converted from the `--force` clap flag at the dispatch
/// boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FreezeGateMode {
    /// Fail closed: refuse the send when the asset is frozen or its freeze
    /// status cannot be confirmed (default).
    Enforce,
    /// Deliberate operator bypass (`--force`); logged as an override.
    Bypass,
}

impl FreezeGateMode {
    pub(super) fn from_force_flag(force: bool) -> Self {
        if force { Self::Bypass } else { Self::Enforce }
    }
}

/// Fail-closed dividend freeze gate for the manual redemption-side commands.
/// V1 automates the dividend lifecycle, so the CLI is incident-response
/// tooling: a frozen (or unconfirmable) asset refuses the send unless the
/// operator deliberately bypasses with `--force`, which is logged.
async fn ensure_not_frozen_for_redemption<Writer: Write>(
    stdout: &mut Writer,
    symbol: &Symbol,
    gate: FreezeGateMode,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    let reader = IssuanceClient::new(
        ctx.issuance.base_url.clone(),
        ctx.issuance.api_key.header_value(),
    )?;

    let outcome = reader.is_frozen(symbol).await;

    match (outcome, gate) {
        (Ok(false), _) => Ok(()),
        (Ok(true), FreezeGateMode::Enforce) => anyhow::bail!(
            "{symbol} is frozen for a dividend; redemption-side sends are \
             held until unfreeze. Re-run with --force to deliberately bypass."
        ),
        (Err(error), FreezeGateMode::Enforce) => anyhow::bail!(
            "could not confirm {symbol} is not frozen ({error}); failing \
             closed. Re-run with --force to deliberately bypass."
        ),
        (Ok(true), FreezeGateMode::Bypass) => {
            warn!(
                target: "rebalance",
                %symbol,
                "Operator --force bypassing the dividend freeze gate for a \
                 FROZEN asset"
            );
            writeln!(
                stdout,
                "⚠️  --force: bypassing dividend freeze for {symbol}"
            )?;
            Ok(())
        }
        (Err(error), FreezeGateMode::Bypass) => {
            warn!(
                target: "rebalance",
                %symbol,
                ?error,
                "Operator --force bypassing the dividend freeze gate with \
                 UNCONFIRMABLE freeze status"
            );
            writeln!(
                stdout,
                "⚠️  --force: freeze status unconfirmable for {symbol}; \
                 proceeding on operator override"
            )?;
            Ok(())
        }
    }
}

/// Arguments for `transfer-equity`, mirroring the CLI surface.
pub(super) struct TransferEquityArgs {
    pub(super) direction: TransferDirection,
    pub(super) symbol: Symbol,
    pub(super) quantity: FractionalShares,
    pub(super) issuer_request_id: Option<Uuid>,
    pub(super) redemption_wallet: Option<Address>,
    pub(super) freeze_gate: FreezeGateMode,
}

pub(super) async fn transfer_equity_command<Writer: Write>(
    stdout: &mut Writer,
    args: TransferEquityArgs,
    ctx: &Ctx,
    pool: &SqlitePool,
) -> anyhow::Result<()> {
    let TransferEquityArgs {
        direction,
        symbol,
        quantity,
        issuer_request_id,
        redemption_wallet: redemption_wallet_flag,
        freeze_gate,
    } = args;
    let symbol = &symbol;
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
            ensure_not_frozen_for_redemption(stdout, symbol, freeze_gate, ctx).await?;

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
/// fund-moving one. The retryable set mirrors the worker: `AttestationTimedOut`,
/// `WithdrawalPollInconclusive` (Alpaca API unreachable or returned a transient
/// error), plus the settlement-wait errors (`WithdrawalTxUnderconfirmed`,
/// `WalletUsdcInsufficient`, `SettlementCheckTransient`). Errors not in this set
/// -- including `AttestationRetryDeadlineElapsed` and a previously-failed
/// aggregate -- are terminal and returned to the caller.
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
            // WithdrawalPollInconclusive is non-terminal: Alpaca was unreachable or
            // returned an error. The aggregate stays in Withdrawing (guard held);
            // the automatic delayed redrive continues in the background. Retry here
            // so the CLI polls periodically without spinning, matching the behaviour
            // of AttestationTimedOut for Circle unavailability.
            Err(UsdcTransferError::WithdrawalPollInconclusive { id, .. }) => {
                warn!(
                    target: "rebalance",
                    %id,
                    ?redrive_delay,
                    "Alpaca withdrawal poll inconclusive; Alpaca may be unreachable -- \
                     retrying after delay (aggregate stays in Withdrawing, guard held)"
                );
                tokio::time::sleep(redrive_delay).await;
            }
            Err(error) => return Err(error),
        }
    }
}

/// Whether a USDC transfer command may start a fresh transfer or must resume an
/// existing one. Drives the `None`-state policy in [`run_usdc_transfer`]: a
/// freshly generated id is expected to have no state (first run), but an
/// operator-supplied resume id that loads to `None` is a wrong/typoed id and
/// must be rejected -- never burned into a brand-new transfer.
#[derive(Clone, Copy)]
enum UsdcTransferStartMode {
    /// First run of a brand-new transfer; the operator supplies the amount.
    Fresh { amount: Usdc },
    /// Re-entry on an existing id; the amount always comes from the persisted
    /// aggregate, never from the CLI.
    Resume,
}

/// Starts a fresh manual USDC transfer. Surfaces the generated id up front so an
/// operator can resume it (via `transfer resume`) if the process is interrupted,
/// then drives it to terminal with attestation-timeout redrive.
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
         transfer resume --kind usdc --id {id} --direction {dir_flag}"
    )?;
    // Flush the recovery id to durable output BEFORE the burn: the resume safety
    // story depends on the operator still having this id if the process is killed
    // after burning, and buffered/redirected stdout could otherwise lose it.
    stdout.flush()?;
    run_usdc_transfer(
        stdout,
        direction,
        id,
        UsdcTransferStartMode::Fresh { amount },
        ctx,
        pool,
    )
    .await
}

/// Resumes an interrupted manual USDC transfer by its id, driving it to terminal
/// with the same attestation-timeout redrive as a fresh transfer. Refuses to run
/// if the id has no persisted transfer (a wrong/typoed id would otherwise start a
/// brand-new burn) or if `--direction` disagrees with the persisted transfer.
/// The amount always comes from the persisted aggregate, never from the CLI.
pub(super) async fn resume_usdc_transfer_command<Writer: Write>(
    stdout: &mut Writer,
    id: Uuid,
    direction: TransferDirection,
    ctx: &Ctx,
    pool: &SqlitePool,
) -> anyhow::Result<()> {
    let id = UsdcRebalanceId(id);
    run_usdc_transfer(
        stdout,
        direction,
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
    id: UsdcRebalanceId,
    mode: UsdcTransferStartMode,
    ctx: &Ctx,
    pool: &SqlitePool,
) -> anyhow::Result<()> {
    let dir = match direction {
        TransferDirection::ToRaindex => "Alpaca -> Raindex",
        TransferDirection::ToAlpaca => "Raindex -> Alpaca",
    };

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
    // mis-drive the aggregate). A resume uses the aggregate's persisted amount --
    // the post-conversion/post-fee effective amount, not the original requested
    // one -- so the CLI never fabricates a financial value for it.
    let amount = match mode {
        UsdcTransferStartMode::Fresh { amount } => {
            writeln!(stdout, "Transferring USDC: {dir}, Amount: {amount} USDC")?;
            amount
        }
        UsdcTransferStartMode::Resume => {
            let Some(state) = usdc_store.load(&id).await? else {
                anyhow::bail!(
                    "transfer resume: no transfer found for id {id}. Refusing to start a new \
                     burn -- check the id and that you are pointed at the right database."
                );
            };

            let expected_direction = match direction {
                TransferDirection::ToRaindex => RebalanceDirection::AlpacaToBase,
                TransferDirection::ToAlpaca => RebalanceDirection::BaseToAlpaca,
            };

            if state.direction() != expected_direction {
                anyhow::bail!(
                    "transfer resume: --direction does not match the persisted transfer for \
                     id {id} (persisted {:?}). Refusing to mis-drive the transfer.",
                    state.direction()
                );
            }

            let amount = state.amount();
            writeln!(
                stdout,
                "Resuming USDC transfer {id}: {dir}, persisted amount: {amount} USDC"
            )?;
            amount
        }
    };

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

    // The CLI has no telemetry writer, so broker dependency samples have nowhere
    // to go; wrap with a disabled sender purely to satisfy the shared
    // `CrossVenueCashTransfer` constructor.
    let alpaca_broker = InstrumentedAlpacaBroker::new(
        AlpacaBrokerApi::try_from_ctx(broker_auth.clone()).await?,
        TelemetrySender::disabled(),
    );

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
        crate::onchain::raindex_contracts(&ctx.evm),
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
        }
        // A recorded pending burn (`pending_burn_tx: Some`) means a CCTP burn was
        // broadcast and durably recorded before its receipt was confirmed. Treat it
        // as POST-burn so `fail-usdc-transfer` cannot clear the guard while a burn
        // may already be on-chain (use `transfer resume`, which checks burn_status
        // and adopts/waits/pages). Only `pending_burn_tx: None` is genuinely pre-burn.
        | UsdcRebalance::BridgingSubmitting {
            pending_burn_tx: Some(_),
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
        | UsdcRebalance::BridgingSubmitting {
            pending_burn_tx: None,
            ..
        } => false,
    };

    if is_post_burn {
        anyhow::bail!(
            "fail-usdc-transfer: transfer {id} is in a post-burn state. \
             A CCTP burn may have already been broadcast. Refusing to act -- \
             use `transfer resume` to adopt/await a recorded in-flight burn \
             (BridgingSubmitting with a recorded tx), `clear-pending-burn` if the \
             recorded burn was dropped and verified absent on-chain, or \
             `transfer reconcile` for a confirmed post-burn failure."
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
        UsdcRebalance::WithdrawalComplete { .. }
        | UsdcRebalance::BridgingSubmitting {
            pending_burn_tx: None,
            ..
        } => {}
        // `BridgingSubmitting { pending_burn_tx: Some(_) }` was already rejected by
        // the post-burn gate above (a recorded burn may be on-chain); listed here
        // for match exhaustiveness.
        UsdcRebalance::BridgingSubmitting {
            pending_burn_tx: Some(_),
            ..
        }
        | UsdcRebalance::Converting { .. }
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
                "fail-usdc-transfer is only valid from BridgingSubmitting (no recorded burn) \
                 or WithdrawalComplete; transfer {id} is in {state:?}. Refusing to act."
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
                 The guard was NOT cleared. Use transfer reconcile for this post-burn \
                 failure. NOTE: transfer reconcile must only be used after confirming \
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

/// Clears a durably-recorded pending CCTP burn hash on a transfer latched at
/// `BridgingSubmitting { pending_burn_tx: Some(_) }`, returning it to
/// `BridgingSubmitting { pending_burn_tx: None }` so the pre-burn
/// `fail-usdc-transfer` path can then release the rebalancing guard.
///
/// A burn classified `Dropped` (`BurnStatus::Dropped` -> `BurnTxDropped`) leaves
/// the aggregate stuck at `BridgingSubmitting` with a recorded burn hash that no
/// other recovery command can clear: `fail-usdc-transfer` treats a recorded burn
/// as post-burn and refuses, `transfer resume` re-derives `Dropped`, and
/// `transfer reconcile` rejects `BridgingSubmitting`. This is the operator
/// escape hatch.
///
/// SAFETY: clearing the recorded hash discards the only durable record that a
/// burn was broadcast. Run this ONLY after verifying on-chain that the burn never
/// landed (the USDC never left the market-maker wallet); clearing it when a burn
/// is actually live would let `fail-usdc-transfer` release the guard while funds
/// are mid-bridge, stranding them. Clearing does NOT release the guard on its own
/// -- after this, run `fail-usdc-transfer` (if the burn never landed) or
/// `transfer resume`.
///
/// Valid ONLY from `BridgingSubmitting { pending_burn_tx: Some(_) }`. Every other
/// state -- including `BridgingSubmitting { pending_burn_tx: None }` (nothing to
/// clear) -- is rejected. Rejects an unknown id. The command is CLI-direct (no
/// running bot required) and is a store-only send (no broker/bridge/provider).
pub(super) async fn clear_pending_burn_command<Writer: Write>(
    stdout: &mut Writer,
    id: Uuid,
    reason: &str,
    pool: &SqlitePool,
) -> anyhow::Result<()> {
    let id = UsdcRebalanceId(id);
    writeln!(
        stdout,
        "Clearing recorded pending CCTP burn for USDC transfer {id} (reason: {reason})"
    )?;

    let usdc_store = StoreBuilder::<UsdcRebalance>::new(pool.clone())
        .build(())
        .await?;

    let Some(state) = usdc_store.load(&id).await? else {
        anyhow::bail!(
            "clear-pending-burn: no transfer found for id {id}. Refusing to act -- \
             check the id and that you are pointed at the right database."
        );
    };

    // Only a `BridgingSubmitting` transfer carrying a recorded pending burn has a
    // hash to clear. A `BridgingSubmitting` with no recorded burn has nothing to
    // clear, and every other state is post-bridge or pre-bridge; reject both with
    // a clear operator-facing error rather than emitting a no-op or surfacing the
    // internal aggregate error.
    //
    // Exhaustive match: new UsdcRebalance variants must be placed explicitly so
    // the compiler flags unhandled variants at this gate boundary.
    match &state {
        UsdcRebalance::BridgingSubmitting {
            pending_burn_tx: Some(_),
            ..
        } => {}
        UsdcRebalance::BridgingSubmitting {
            pending_burn_tx: None,
            ..
        } => {
            anyhow::bail!(
                "clear-pending-burn: transfer {id} is at BridgingSubmitting with no recorded \
                 pending burn to clear (pending_burn_tx: None). If the guard must be released, \
                 use fail-usdc-transfer (after verifying on-chain that no burn landed)."
            );
        }
        UsdcRebalance::Converting { .. }
        | UsdcRebalance::ConversionComplete { .. }
        | UsdcRebalance::ConversionFailed { .. }
        | UsdcRebalance::WithdrawalSubmitting { .. }
        | UsdcRebalance::Withdrawing { .. }
        | UsdcRebalance::WithdrawalFailed { .. }
        | UsdcRebalance::WithdrawalComplete { .. }
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
                "clear-pending-burn is only valid from BridgingSubmitting with a recorded \
                 pending burn; transfer {id} is in {state:?}. Refusing to act."
            );
        }
    }

    usdc_store
        .send(&id, UsdcRebalanceCommand::ClearPendingBurn)
        .await?;

    writeln!(
        stdout,
        "Cleared the recorded pending burn for USDC transfer {id}; it is now at \
         BridgingSubmitting with no recorded burn (the rebalancing guard is still held). \
         Next, run fail-usdc-transfer (if you confirmed on-chain that the burn never landed) \
         to release the guard, or `transfer resume --kind usdc` to continue the bridge."
    )?;

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
            "transfer reconcile: no transfer found for id {id}. Refusing to act -- check \
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
            "transfer reconcile: transfer {id} is in state {state:?}, not a post-burn \
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
    recipient: Option<Address>,
    ctx: &Ctx,
    provider: Prov,
) -> anyhow::Result<()> {
    writeln!(stdout, "🔄 Requesting tokenization via Alpaca API")?;
    writeln!(stdout, "   Symbol: {symbol}")?;
    writeln!(stdout, "   Quantity: {quantity}")?;

    let token = ctx
        .assets
        .tokenized_equity(&symbol)
        .ok_or_else(|| anyhow::anyhow!("equity {symbol} is not configured in [assets.equities]"))?;
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

    writeln!(stdout, "   Request ID: {}", request.id)?;
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
    redemption_wallet_flag: Option<Address>,
    freeze_gate: FreezeGateMode,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    writeln!(stdout, "🔄 Requesting redemption via Alpaca API")?;
    writeln!(stdout, "   Symbol: {symbol}")?;
    writeln!(stdout, "   Quantity: {quantity}")?;

    let token = ctx
        .assets
        .tokenized_equity(&symbol)
        .ok_or_else(|| anyhow::anyhow!("equity {symbol} is not configured in [assets.equities]"))?;
    writeln!(stdout, "   Token: {token}")?;

    ensure_not_frozen_for_redemption(stdout, &symbol, freeze_gate, ctx).await?;

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

    writeln!(stdout, "   Request ID: {}", request.id)?;
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
    writeln!(stdout, "   ID:       {}", request.id)?;
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

/// Adds operator guidance to a rejected recovery command: between the CLI's
/// state read and the command dispatch, the running bot may have advanced the
/// aggregate, so on a typed rejection the operator should re-run to see the
/// current state. Infrastructure failures pass through without the hint.
fn stale_state_context<Failure: std::error::Error + Send + Sync + 'static>(
    kind: &str,
    id: &str,
    error: AggregateError<Failure>,
) -> anyhow::Error {
    match error {
        rejection @ (AggregateError::UserError(_) | AggregateError::AggregateConflict) => {
            anyhow::Error::new(rejection).context(format!(
                "{kind} {id} rejected the failure command. The state may have \
                 advanced since it was read (is the bot driving this aggregate \
                 concurrently?) -- re-run to see the current state."
            ))
        }
        infrastructure @ (AggregateError::DatabaseConnectionError(_)
        | AggregateError::DeserializationError(_)
        | AggregateError::UnexpectedError(_)) => anyhow::Error::new(infrastructure),
    }
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
                // A mint stuck at MintRequested (requested at the provider but
                // never accepted) is force-failed via FailAcceptance, which the
                // aggregate now accepts from MintRequested.
                MintRequested { .. } | MintAccepted { .. } => FailAcceptance {
                    reason: reason.to_string(),
                },
                TokensReceived { .. } | WrapSubmitted { .. } => FailWrapping {
                    reason: reason.to_string(),
                },
                TokensWrapped { .. } | VaultDepositSubmitted { .. } => FailRaindexDeposit {
                    reason: reason.to_string(),
                },
                DepositedIntoRaindex { .. } => {
                    anyhow::bail!("Mint {id} already completed (DepositedIntoRaindex)");
                }
                Failed { .. } => {
                    anyhow::bail!("Mint {id} already failed");
                }
                Reconciled { .. } => {
                    anyhow::bail!("Mint {id} already reconciled");
                }
            };

            st0x_event_sorcery::send_command::<TokenizedEquityMint>(pool, &mint_id, command, ())
                .await
                .map_err(|error| stale_state_context("Mint", id, error))?;

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
            use EquityRedemptionCommand::*;
            let command = match entity {
                VaultWithdrawPending { .. }
                | VaultWithdrawSubmitted { .. }
                | WithdrawnFromRaindex { .. }
                | UnwrapPending { .. }
                | UnwrapSubmitted { .. }
                | TokensUnwrapped { .. }
                | SendPending { .. } => FailTransfer {
                    reason: reason.to_string(),
                },
                // The redemption transfer was broadcast and is finalizing to
                // `TokensSent` (a pure, resume-driven step). Failing it here would
                // mis-record a broadcast transfer as never-sent; let it finalize,
                // then fail from `TokensSent` if detection never fires.
                SendSubmitted { .. } => {
                    anyhow::bail!(
                        "Redemption {id} is mid-send (transfer broadcast, finalizing); \
                         re-run once it reaches TokensSent"
                    );
                }
                // Tokens reached Alpaca's redemption wallet but detection never
                // fired: force the detection-failure terminal (DetectionFailed
                // -> Failed), persisting the operator's reason on the event.
                TokensSent { .. } => FailDetection {
                    failure: DetectionFailure::Operator {
                        reason: reason.to_string(),
                    },
                },
                // Alpaca detected the transfer but never completed it: reject
                // the redemption (RedemptionRejected -> Failed).
                Pending { .. } => RejectRedemption {
                    reason: reason.to_string(),
                },
                Completed { .. } => {
                    anyhow::bail!("Redemption {id} already completed");
                }
                Failed { .. } => {
                    anyhow::bail!("Redemption {id} already failed");
                }
                Reconciled { .. } => {
                    anyhow::bail!("Redemption {id} already reconciled");
                }
            };

            st0x_event_sorcery::send_command::<EquityRedemption>(pool, &redemption_id, command, ())
                .await
                .map_err(|error| stale_state_context("Redemption", id, error))?;

            writeln!(
                stdout,
                "Redemption {id} marked as failed (reason: {reason})"
            )?;
        }
    }

    Ok(())
}

/// Wraps a reqwest send error: a connection failure gets actionable operator
/// guidance (the bot is probably not running), anything else passes through.
fn connect_error(url: &str, error: reqwest::Error) -> anyhow::Error {
    if error.is_connect() {
        anyhow::Error::new(error)
            .context(format!("could not reach the bot at {url}; is it running?"))
    } else {
        anyhow::Error::new(error)
    }
}

/// Reconciles a mint or redemption stranded in the terminal `Failed` state to
/// the terminal `Reconciled` state.
///
/// Loads the aggregate, verifies it is in `Failed` (bails with a clear operator
/// error otherwise -- the aggregate command also gates this, but the preflight
/// gives a clearer message first), and sends the `Reconcile` command. This is a
/// pure bookkeeping resolution: the residual equity was handled out-of-band
/// (e.g. via wrap-equity/vault-deposit), so there is no inventory side-effect.
pub(crate) async fn reconcile_equity_transfer_command<W: Write>(
    stdout: &mut W,
    transfer_type: TransferType,
    id: &str,
    reason: String,
    pool: &SqlitePool,
) -> anyhow::Result<()> {
    match transfer_type {
        TransferType::Mint => {
            let mint_id: IssuerRequestId = id.parse().map_err(|error| {
                anyhow::anyhow!("transfer reconcile: invalid mint id {id:?}: {error}")
            })?;

            let entity = st0x_event_sorcery::load_entity::<TokenizedEquityMint>(pool, &mint_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("Mint aggregate not found: {id}"))?;

            if !matches!(entity, TokenizedEquityMint::Failed { .. }) {
                anyhow::bail!(
                    "transfer reconcile: mint {id} is not in the Failed state. Refusing to act \
                     -- reconcile only resolves a transfer stuck in the Failed terminal; check \
                     its current state on the dashboard."
                );
            }

            st0x_event_sorcery::send_command::<TokenizedEquityMint>(
                pool,
                &mint_id,
                TokenizedEquityMintCommand::Reconcile { reason },
                (),
            )
            .await?;

            writeln!(stdout, "Mint {id} reconciled")?;
        }

        TransferType::Redemption => {
            let redemption_id: RedemptionAggregateId = id.parse().map_err(|error| {
                anyhow::anyhow!("transfer reconcile: invalid redemption id {id:?}: {error}")
            })?;

            let entity = st0x_event_sorcery::load_entity::<EquityRedemption>(pool, &redemption_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("Redemption aggregate not found: {id}"))?;

            if !matches!(entity, EquityRedemption::Failed { .. }) {
                anyhow::bail!(
                    "transfer reconcile: redemption {id} is not in the Failed state. Refusing to \
                     act -- reconcile only resolves a transfer stuck in the Failed terminal; \
                     check its current state on the dashboard."
                );
            }

            st0x_event_sorcery::send_command::<EquityRedemption>(
                pool,
                &redemption_id,
                EquityRedemptionCommand::Reconcile { reason },
                (),
            )
            .await?;

            writeln!(
                stdout,
                "Redemption {id} reconciled. If it had ended in DetectionFailed/RedemptionRejected, \
                 its stranded exposure was seeded into live inflight at startup; reconcile only \
                 stops it being re-seeded on the next restart, so a running bot still needs a \
                 restart before rebalancing resumes for this symbol."
            )?;
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
    let response = client
        .post(url.as_str())
        .send()
        .await
        .map_err(|error| connect_error(&url, error))?;
    let status = response.status();
    let body = response.text().await?;

    if !status.is_success() {
        anyhow::bail!("transfer recheck failed ({status}): {body}");
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
    writeln!(stdout, "transfer recheck outcome: {outcome}")?;
    Ok(())
}

/// Resumes ALL interrupted equity transfers (mints and redemptions) by calling
/// the running bot's `/transfers/resume` endpoint. The endpoint always resumes
/// ALL interrupted transfers (each independently; failures reported as counts)
/// (no per-id/per-kind filter) and must run in the bot process so recovery
/// dispatches through the reactor-wired store and shares the bot's resume lock.
/// Requires the bot to be running and serving its REST API on `server_port`.
pub(crate) async fn resume_interrupted_transfers_command<W: Write>(
    stdout: &mut W,
    ctx: &Ctx,
) -> anyhow::Result<()> {
    let url = format!("http://127.0.0.1:{}/transfers/resume", ctx.server_port);
    writeln!(
        stdout,
        "Resuming all interrupted equity transfers via {url}"
    )?;

    // Bound only the CONNECT phase: the bot drives every interrupted
    // transfer's on-chain recovery inline before responding (confirmations,
    // vault deposits), so a total request timeout would cancel the in-flight
    // handler mid-recovery and report a false failure. Once connected, wait as
    // long as the recovery takes. Deliberate tradeoff: if the bot stalls after
    // accepting the connection, this invocation hangs until the operator kills
    // it -- preferable to silently cancelling real on-chain recovery work,
    // whose duration has no known upper bound (N transfers x confirmations).
    let client = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(5))
        .build()?;
    let response = client
        .post(url.as_str())
        .send()
        .await
        .map_err(|error| connect_error(&url, error))?;
    let status = response.status();
    let body = response.text().await?;

    if !status.is_success() {
        anyhow::bail!("transfer resume failed ({status}): {body}");
    }

    let resumed: ResumeResponse = serde_json::from_str(&body).map_err(|error| {
        anyhow::Error::new(error).context(format!("could not parse resume response '{body}'"))
    })?;
    writeln!(
        stdout,
        "Resumed {} mint(s) ({} failed), {} redemption(s) ({} failed)",
        resumed.mints_attempted,
        resumed.mints_failed,
        resumed.redemptions_attempted,
        resumed.redemptions_failed
    )?;

    let failed = resumed.mints_failed + resumed.redemptions_failed;
    if failed > 0 {
        anyhow::bail!(
            "{failed} transfer(s) failed to resume; check the bot logs for per-id errors"
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, TxHash, address, b256};
    use alloy::providers::ProviderBuilder;
    use rain_math_float::Float;
    use url::Url;
    use uuid::uuid;

    use st0x_config::ExecutionThreshold;
    use st0x_config::RebalancingCtx;
    use st0x_config::create_test_issuance_ctx;
    use st0x_config::{
        AssetsConfig, CashAssetConfig, EquitiesConfig, EquityAssetConfig, LogLevel, OperationMode,
        TradingMode,
    };
    use st0x_config::{EvmCtx, IngestionCutoff, InventoryMode};
    use st0x_event_sorcery::LifecycleError;
    use st0x_execution::{
        AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode, AlpacaTransferId,
        AlpacaWalletError, ClientOrderId, TimeInForce,
    };
    use st0x_finance::Usdc;
    use st0x_float_macro::float;
    use st0x_tokenization::{issuer_request_id, tokenization_request_id};

    use super::*;
    use crate::equity_redemption::{EquityRedemptionError, redemption_aggregate_id};
    use crate::inventory::ImbalanceThreshold;
    use crate::test_utils::setup_test_db;
    use crate::usdc_rebalance::{ReconcileReason, TransferRef, UsdcRebalanceCommand};

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

    /// The manual redrive loop must also retry `WithdrawalPollInconclusive` --
    /// otherwise a manual `stox transfer resume --kind usdc --direction to-raindex`
    /// invocation would exit when Alpaca is temporarily unreachable instead of
    /// waiting for recovery, leaving the operator unable to drive the stuck
    /// withdrawal to completion via the CLI.
    #[tokio::test]
    async fn redrive_loop_retries_withdrawal_poll_inconclusive_then_succeeds() {
        let calls = std::cell::Cell::new(0u32);

        let result = redrive_transfer_until_settled(Duration::from_millis(0), || {
            let attempt = calls.get() + 1;
            calls.set(attempt);
            async move {
                if attempt == 1 {
                    Err(UsdcTransferError::WithdrawalPollInconclusive {
                        id: UsdcRebalanceId(Uuid::from_u128(42)),
                        initiated_at: chrono::Utc::now(),
                        source: AlpacaWalletError::TransferTimeout {
                            transfer_id: AlpacaTransferId::from(Uuid::from_u128(1)),
                            elapsed: std::time::Duration::from_secs(1800),
                        },
                    })
                } else {
                    Ok(())
                }
            }
        })
        .await;

        result.expect("redrive must succeed once Alpaca becomes reachable again");
        assert_eq!(
            calls.get(),
            2,
            "the loop must retry exactly once after the inconclusive poll, then succeed",
        );
    }

    /// `transfer resume --kind equity` against a mocked bot endpoint: the
    /// summary line renders the counts from a literal response body.
    #[tokio::test]
    async fn resume_interrupted_transfers_reports_counts() {
        let server = httpmock::MockServer::start_async().await;
        let mock = server
            .mock_async(|when, then| {
                when.method(httpmock::Method::POST).path("/transfers/resume");
                then.status(200).body(
                    r#"{"mintsAttempted":2,"mintsFailed":0,"redemptionsAttempted":1,"redemptionsFailed":0}"#,
                );
            })
            .await;
        let mut ctx = create_ctx_without_rebalancing();
        ctx.server_port = server.port();

        let mut stdout = Vec::new();
        resume_interrupted_transfers_command(&mut stdout, &ctx)
            .await
            .unwrap();

        mock.assert_async().await;
        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("Resumed 2 mint(s) (0 failed), 1 redemption(s) (0 failed)"),
            "unexpected output: {output}"
        );
    }

    /// Per-transfer failures must surface as a non-zero exit, not a clean one:
    /// scripts and operators must not mistake a partial recovery for success.
    #[tokio::test]
    async fn resume_interrupted_transfers_fails_on_partial_failures() {
        let server = httpmock::MockServer::start_async().await;
        server
            .mock_async(|when, then| {
                when.method(httpmock::Method::POST).path("/transfers/resume");
                then.status(200).body(
                    r#"{"mintsAttempted":2,"mintsFailed":1,"redemptionsAttempted":1,"redemptionsFailed":1}"#,
                );
            })
            .await;
        let mut ctx = create_ctx_without_rebalancing();
        ctx.server_port = server.port();

        let mut stdout = Vec::new();
        let error = resume_interrupted_transfers_command(&mut stdout, &ctx)
            .await
            .unwrap_err();

        assert!(
            error.to_string().contains("2 transfer(s) failed to resume"),
            "expected the failure-count bail; got: {error}"
        );
        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("Resumed 2 mint(s) (1 failed), 1 redemption(s) (1 failed)"),
            "the counts must still be printed before bailing; got: {output}"
        );
    }

    /// A failure on only one side (mints) must still be a non-zero exit.
    #[tokio::test]
    async fn resume_interrupted_transfers_fails_when_only_mints_fail() {
        let server = httpmock::MockServer::start_async().await;
        server
            .mock_async(|when, then| {
                when.method(httpmock::Method::POST).path("/transfers/resume");
                then.status(200).body(
                    r#"{"mintsAttempted":1,"mintsFailed":1,"redemptionsAttempted":0,"redemptionsFailed":0}"#,
                );
            })
            .await;
        let mut ctx = create_ctx_without_rebalancing();
        ctx.server_port = server.port();

        let mut stdout = Vec::new();
        let error = resume_interrupted_transfers_command(&mut stdout, &ctx)
            .await
            .unwrap_err();

        assert!(
            error.to_string().contains("1 transfer(s) failed to resume"),
            "a mint-only failure must be non-zero exit; got: {error}"
        );
        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("Resumed 1 mint(s) (1 failed), 0 redemption(s) (0 failed)"),
            "the counts must still be printed before bailing; got: {output}"
        );
    }

    /// A 200 with an unparseable body must fail loudly with the raw body.
    #[tokio::test]
    async fn resume_interrupted_transfers_rejects_malformed_response() {
        let server = httpmock::MockServer::start_async().await;
        server
            .mock_async(|when, then| {
                when.method(httpmock::Method::POST)
                    .path("/transfers/resume");
                then.status(200).body(r#"{"unexpected":1}"#);
            })
            .await;
        let mut ctx = create_ctx_without_rebalancing();
        ctx.server_port = server.port();

        let mut stdout = Vec::new();
        let error = resume_interrupted_transfers_command(&mut stdout, &ctx)
            .await
            .unwrap_err();

        let chain = format!("{error:#}");
        assert!(
            chain.contains("could not parse resume response"),
            "expected the parse-failure context; got: {chain}"
        );
        assert!(
            chain.contains(r#"{"unexpected":1}"#),
            "the raw body must be surfaced; got: {chain}"
        );
    }

    /// The bot's real lock-held response (409 + ErrorResponse JSON) must reach
    /// the operator verbatim.
    #[tokio::test]
    async fn resume_interrupted_transfers_surfaces_lock_conflict() {
        let server = httpmock::MockServer::start_async().await;
        server
            .mock_async(|when, then| {
                when.method(httpmock::Method::POST)
                    .path("/transfers/resume");
                then.status(409)
                    .body(r#"{"error":"A resume operation is already in progress"}"#);
            })
            .await;
        let mut ctx = create_ctx_without_rebalancing();
        ctx.server_port = server.port();

        let mut stdout = Vec::new();
        let error = resume_interrupted_transfers_command(&mut stdout, &ctx)
            .await
            .unwrap_err();

        let message = error.to_string();
        assert!(message.contains("transfer resume failed"), "got: {message}");
        assert!(message.contains("409"), "got: {message}");
        assert!(
            message.contains("A resume operation is already in progress"),
            "got: {message}"
        );
    }

    /// A connection refusal (bot not running) must carry the actionable hint.
    #[tokio::test]
    async fn resume_interrupted_transfers_hints_when_bot_unreachable() {
        // Bind and immediately drop a listener to get a port with nothing
        // listening on it.
        let port = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr().unwrap().port()
        };
        let mut ctx = create_ctx_without_rebalancing();
        ctx.server_port = port;

        let mut stdout = Vec::new();
        let error = resume_interrupted_transfers_command(&mut stdout, &ctx)
            .await
            .unwrap_err();

        let chain = format!("{error:#}");
        assert!(
            chain.contains("is it running?"),
            "a refused connection must carry the bot hint; got: {chain}"
        );
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
                inventory: InventoryMode::Managed {
                    inventory: address!("0x1234567890123456789012345678901234567890"),
                },
                vault_owner: Address::ZERO,
                deployment_block: 1,
                required_confirmations: 0,
                ingestion_cutoff: IngestionCutoff::Safe,
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
                inventory: InventoryMode::Managed {
                    inventory: address!("0x1234567890123456789012345678901234567890"),
                },
                vault_owner: Address::ZERO,
                deployment_block: 1,
                required_confirmations: 0,
                ingestion_cutoff: IngestionCutoff::Safe,
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
            TransferEquityArgs {
                direction: TransferDirection::ToRaindex,
                symbol,
                quantity,
                issuer_request_id: None,
                redemption_wallet: None,
                freeze_gate: FreezeGateMode::Enforce,
            },
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
            TransferEquityArgs {
                direction: TransferDirection::ToRaindex,
                symbol,
                quantity,
                issuer_request_id: None,
                redemption_wallet: None,
                freeze_gate: FreezeGateMode::Enforce,
            },
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
        // The core safety contract of `transfer resume`: an id that has no
        // persisted transfer (a typo, or the wrong database) MUST be rejected up
        // front rather than falling through to the manager's `None` -> fresh-burn
        // path. The existence check runs before any broker/bridge setup, so a bare
        // ctx and an empty pool reach it directly.
        let ctx = create_ctx_without_rebalancing();
        let pool = setup_test_db().await;
        let unknown_id = Uuid::from_u128(0xDEAD_BEEF);

        let mut stdout = Vec::new();
        let result = resume_usdc_transfer_command(
            &mut stdout,
            unknown_id,
            TransferDirection::ToRaindex,
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
        let result =
            resume_usdc_transfer_command(&mut stdout, id, TransferDirection::ToAlpaca, &ctx, &pool)
                .await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("does not match the persisted transfer"),
            "resume with the wrong direction must be rejected, not mis-drive; got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn resume_usdc_transfer_reports_persisted_amount() {
        // A resume takes no amount: it loads the persisted state amount (the
        // post-slippage/post-fee effective amount) and reports it to the
        // operator. The preflight guard must accept the correct direction --
        // here it then fails at broker setup, proving the guard accepted it.
        let ctx = create_ctx_without_rebalancing();
        let pool = setup_test_db().await;
        let seeded_amount = Usdc::new(Float::parse("100".to_string()).unwrap());
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
            &ctx,
            &pool,
        )
        .await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            !err_msg.contains("does not match"),
            "the correct direction must pass the guard; got: {err_msg}"
        );
        assert!(
            err_msg.contains("requires Alpaca Broker API configuration"),
            "past the guard, the bare ctx must fail at broker setup; got: {err_msg}"
        );

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("persisted amount: 100 USDC"),
            "the resume must report the persisted effective amount; got: {output}"
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
            err_msg.contains(
                "only valid from BridgingSubmitting (no recorded burn) or WithdrawalComplete"
            ),
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

    /// `fail-usdc-transfer` MUST be rejected when a CCTP burn was already recorded
    /// (`BridgingSubmitting { pending_burn_tx: Some(_) }`): failing it as pre-burn
    /// would emit `BridgingFailed { burn_tx_hash: None }`, clearing the double-burn
    /// guard while a burn may already be on-chain (stranded funds / double burn).
    #[tokio::test]
    async fn fail_usdc_transfer_rejects_bridging_submitting_with_recorded_burn() {
        let pool = setup_test_db().await;
        let id = Uuid::from_u128(0xBEEF_0009);

        let store = StoreBuilder::<UsdcRebalance>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        seed_to_bridging_submitting(&store, id).await;
        let burn_tx = alloy::primitives::TxHash::from([0x77; 32]);
        store
            .send(
                &UsdcRebalanceId(id),
                UsdcRebalanceCommand::RecordPendingBurn { burn_tx },
            )
            .await
            .unwrap();

        let mut stdout = Vec::new();
        let error = fail_usdc_transfer_command(&mut stdout, id, "manual fail", &pool)
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("post-burn"),
            "fail-usdc-transfer must reject a recorded-burn BridgingSubmitting as post-burn; \
             got: {error}"
        );

        // State unchanged: still BridgingSubmitting with the recorded burn; guard held.
        let state = store.load(&UsdcRebalanceId(id)).await.unwrap().unwrap();
        assert!(
            matches!(
                state,
                UsdcRebalance::BridgingSubmitting { pending_burn_tx: Some(tx), .. } if tx == burn_tx
            ),
            "state must remain BridgingSubmitting with the recorded burn (not failed); \
             got: {state:?}"
        );
        assert!(
            state.holds_rebalance_guard(),
            "a recorded-burn BridgingSubmitting must still hold the rebalancing guard"
        );
    }

    /// `clear-pending-burn` is the operator escape hatch for a transfer latched at
    /// `BridgingSubmitting { pending_burn_tx: Some(_) }` after a `Dropped`
    /// classification: it clears the recorded hash so the pre-burn
    /// `fail-usdc-transfer` path can then release the guard. Clearing itself must
    /// NOT release the guard -- `BridgingSubmitting` keeps holding it.
    #[tokio::test]
    async fn clear_pending_burn_succeeds_on_bridging_submitting_with_recorded_burn() {
        let pool = setup_test_db().await;
        let id = Uuid::from_u128(0xBEEF_000C);

        let store = StoreBuilder::<UsdcRebalance>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        seed_to_bridging_submitting(&store, id).await;
        let burn_tx = alloy::primitives::TxHash::from([0x55; 32]);
        store
            .send(
                &UsdcRebalanceId(id),
                UsdcRebalanceCommand::RecordPendingBurn { burn_tx },
            )
            .await
            .unwrap();

        let mut stdout = Vec::new();
        let result = clear_pending_burn_command(&mut stdout, id, "burn never landed", &pool).await;
        result.expect("clear_pending_burn must succeed from BridgingSubmitting with a record");

        let state = store.load(&UsdcRebalanceId(id)).await.unwrap().unwrap();
        assert!(
            matches!(
                state,
                UsdcRebalance::BridgingSubmitting {
                    pending_burn_tx: None,
                    ..
                }
            ),
            "clearing must return BridgingSubmitting with no recorded burn; got: {state:?}"
        );
        assert!(
            state.holds_rebalance_guard(),
            "clearing the pending burn must NOT release the rebalancing guard"
        );

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("fail-usdc-transfer"),
            "success message must point the operator at fail-usdc-transfer; got: {output}"
        );
    }

    /// A `BridgingSubmitting` with no recorded burn has nothing to clear; the
    /// command must reject it rather than emit a no-op.
    #[tokio::test]
    async fn clear_pending_burn_rejects_bridging_submitting_without_recorded_burn() {
        let pool = setup_test_db().await;
        let id = Uuid::from_u128(0xBEEF_000D);

        let store = StoreBuilder::<UsdcRebalance>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        seed_to_bridging_submitting(&store, id).await;

        let mut stdout = Vec::new();
        let error = clear_pending_burn_command(&mut stdout, id, "nothing to clear", &pool)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("no recorded pending burn to clear"),
            "clear-pending-burn must reject a BridgingSubmitting with no recorded burn; \
             got: {error}"
        );

        // State unchanged: still BridgingSubmitting with no recorded burn.
        let state = store.load(&UsdcRebalanceId(id)).await.unwrap().unwrap();
        assert!(
            matches!(
                state,
                UsdcRebalance::BridgingSubmitting {
                    pending_burn_tx: None,
                    ..
                }
            ),
            "state must remain BridgingSubmitting with no recorded burn; got: {state:?}"
        );
    }

    #[tokio::test]
    async fn clear_pending_burn_rejects_unknown_id() {
        let pool = setup_test_db().await;
        let unknown_id = Uuid::from_u128(0xC0FF_EE01);

        let mut stdout = Vec::new();
        let result = clear_pending_burn_command(&mut stdout, unknown_id, "reason", &pool).await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("no transfer found for id"),
            "unknown id must be rejected; got: {err_msg}"
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
        // must reject it so the operator is routed to transfer reconcile.
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
            err_msg.contains(
                "only valid from BridgingSubmitting (no recorded burn) or WithdrawalComplete"
            ),
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
            err_msg.contains(
                "only valid from BridgingSubmitting (no recorded burn) or WithdrawalComplete"
            ),
            "WithdrawalFailed state must be rejected as not in bridging phase; got: {err_msg}"
        );
    }

    #[test]
    fn tokenized_equity_resolves_from_config() {
        let mut ctx = create_ctx_without_rebalancing();
        let token = address!("0x626757e6f50675d17fcad312e82f989ae7a23d38");
        ctx.assets.equities.symbols.insert(
            Symbol::new("COIN").unwrap(),
            EquityAssetConfig {
                tokenized_equity: token,
                tokenized_equity_derivative: Address::ZERO,
                pyth_feed_id: None,
                vault_ids: Vec::new(),
                trading: OperationMode::Enabled,
                rebalancing: OperationMode::Disabled,
                wrapped_equity_recovery: OperationMode::Disabled,
                extended_hours_counter_trading: OperationMode::Disabled,
                operational_limit: None,
            },
        );

        assert_eq!(
            ctx.assets.tokenized_equity(&Symbol::new("COIN").unwrap()),
            Some(token),
            "a configured symbol must resolve to its tokenized_equity address",
        );
        assert_eq!(
            ctx.assets.tokenized_equity(&Symbol::new("AAPL").unwrap()),
            None,
            "an unconfigured symbol must resolve to None, never a default address",
        );
    }

    #[tokio::test]
    async fn alpaca_tokenize_fails_when_symbol_not_configured() {
        let ctx = create_alpaca_ctx_without_rebalancing();
        let provider =
            ProviderBuilder::new().connect_http(Url::parse("http://localhost:8545").unwrap());
        let mut stdout = Vec::new();

        let error = alpaca_tokenize_command(
            &mut stdout,
            Symbol::new("COIN").unwrap(),
            FractionalShares::new(float!(10)),
            None,
            &ctx,
            provider,
        )
        .await
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("equity COIN is not configured in [assets.equities]"),
            "an unconfigured symbol must fail before any network call, got: {error}"
        );
    }

    /// Points the ctx's issuance endpoint at an httpmock server reporting the
    /// given freeze status for AAPL.
    fn ctx_with_issuance_status(server: &httpmock::MockServer, status: &str) -> Ctx {
        server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/tokenized-assets/AAPL/status");
            then.status(200).json_body(serde_json::json!({
                "underlying": "AAPL",
                "status": status,
            }));
        });

        let mut ctx = create_alpaca_ctx_without_rebalancing();
        ctx.assets.equities.symbols.insert(
            Symbol::new("AAPL").unwrap(),
            EquityAssetConfig {
                tokenized_equity: address!("0x626757e6f50675d17fcad312e82f989ae7a23d38"),
                tokenized_equity_derivative: Address::ZERO,
                pyth_feed_id: None,
                vault_ids: Vec::new(),
                trading: OperationMode::Enabled,
                rebalancing: OperationMode::Disabled,
                wrapped_equity_recovery: OperationMode::Disabled,
                extended_hours_counter_trading: OperationMode::Disabled,
                operational_limit: None,
            },
        );
        ctx.issuance =
            st0x_config::test_issuance_status_ctx(Url::parse(&server.base_url()).unwrap());
        ctx
    }

    // The dividend freeze gate on the manual redemption-side command: a
    // frozen asset refuses the send, and --force is a logged, deliberate
    // operator bypass.
    #[tokio::test]
    async fn alpaca_redeem_fails_closed_when_asset_frozen() {
        let server = httpmock::MockServer::start_async().await;
        let ctx = ctx_with_issuance_status(&server, "frozen");
        let mut stdout = Vec::new();

        let error = alpaca_redeem_command(
            &mut stdout,
            Symbol::new("AAPL").unwrap(),
            FractionalShares::new(float!(10)),
            None,
            FreezeGateMode::Enforce,
            &ctx,
        )
        .await
        .unwrap_err();

        let message = error.to_string();
        assert!(
            message.contains("frozen for a dividend") && message.contains("--force"),
            "a frozen asset must fail closed and point at --force, got: {message}"
        );
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn alpaca_redeem_force_bypasses_freeze_gate_with_logged_override() {
        let server = httpmock::MockServer::start_async().await;
        let ctx = ctx_with_issuance_status(&server, "frozen");
        let mut stdout = Vec::new();

        let result = alpaca_redeem_command(
            &mut stdout,
            Symbol::new("AAPL").unwrap(),
            FractionalShares::new(float!(10)),
            None,
            FreezeGateMode::Bypass,
            &ctx,
        )
        .await;

        // Past the gate the fixture ctx lacks wallet config, so the command
        // errors later -- what matters is the freeze gate itself was bypassed
        // and the override was logged and printed.
        let message = result.unwrap_err().to_string();
        assert!(
            !message.contains("frozen for a dividend"),
            "--force must bypass the freeze refusal, got: {message}"
        );

        let printed = String::from_utf8(stdout).unwrap();
        assert!(
            printed.contains("bypassing dividend freeze for AAPL"),
            "the bypass must be visible in the command output, got: {printed}"
        );
        assert!(logs_contain(
            "Operator --force bypassing the dividend freeze gate"
        ));
    }

    #[tokio::test]
    async fn alpaca_redeem_fails_when_symbol_not_configured() {
        let ctx = create_alpaca_ctx_without_rebalancing();
        let mut stdout = Vec::new();

        let error = alpaca_redeem_command(
            &mut stdout,
            Symbol::new("COIN").unwrap(),
            FractionalShares::new(float!(10)),
            None,
            FreezeGateMode::Enforce,
            &ctx,
        )
        .await
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("equity COIN is not configured in [assets.equities]"),
            "an unconfigured symbol must fail before any network call, got: {error}"
        );
    }

    /// Drives a redemption through the real CQRS command flow until the vault
    /// withdrawal is confirmed (`WithdrawnFromRaindex`) -- stuck before the
    /// tokens leave the bot's custody.
    async fn seed_redemption_to_withdrawn(pool: &SqlitePool, id: &RedemptionAggregateId) {
        use EquityRedemptionCommand::*;

        let store = StoreBuilder::<EquityRedemption>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        store
            .send(
                id,
                Redeem {
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: float!(50.25),
                    token: Address::random(),
                    amount: U256::from(50_250_000_000_000_000_000_u128),
                },
            )
            .await
            .unwrap();
        store
            .send(
                id,
                SubmitWithdraw {
                    tx_hash: TxHash::random(),
                },
            )
            .await
            .unwrap();
        store
            .send(
                id,
                ConfirmWithdraw {
                    actual_wrapped_amount: U256::from(50_250_000_000_000_000_000_u128),
                    raindex_withdraw_block: 1,
                },
            )
            .await
            .unwrap();
    }

    /// Continues the real CQRS command flow until the redemption is stuck in
    /// `TokensSent`: tokens reached Alpaca's redemption wallet but detection
    /// never fired.
    async fn seed_redemption_to_tokens_sent(pool: &SqlitePool, id: &RedemptionAggregateId) {
        use EquityRedemptionCommand::*;

        seed_redemption_to_withdrawn(pool, id).await;

        let store = StoreBuilder::<EquityRedemption>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        store.send(id, UnwrapTokens).await.unwrap();
        store
            .send(
                id,
                SubmitUnwrap {
                    unwrap_tx_hash: TxHash::random(),
                },
            )
            .await
            .unwrap();
        store
            .send(
                id,
                ConfirmUnwrap {
                    underlying_token: Address::random(),
                    unwrapped_amount: U256::from(50_250_000_000_000_000_000_u128),
                    unwrap_block: 2,
                },
            )
            .await
            .unwrap();
        store.send(id, PrepareSend).await.unwrap();
        store
            .send(
                id,
                RecordSendOutcome {
                    outcome: crate::equity_redemption::SendOutcome::Sent {
                        redemption_wallet: Address::random(),
                        redemption_tx: TxHash::random(),
                    },
                },
            )
            .await
            .unwrap();
    }

    async fn send_redemption_command(
        pool: &SqlitePool,
        id: &RedemptionAggregateId,
        command: EquityRedemptionCommand,
    ) {
        let store = StoreBuilder::<EquityRedemption>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        store.send(id, command).await.unwrap();
    }

    /// `transfer fail --kind redemption` on a redemption stuck in `TokensSent`
    /// must dispatch `FailDetection { Operator }` and persist the operator's
    /// `--reason`, recoverable from the replayed `Failed` state.
    #[tokio::test]
    async fn fail_transfer_redemption_tokens_sent_persists_operator_reason() {
        let pool = setup_test_db().await;
        let id = redemption_aggregate_id("cli-stuck-tokens-sent");
        seed_redemption_to_tokens_sent(&pool, &id).await;

        let mut stdout = Vec::new();
        fail_transfer_command(
            &mut stdout,
            &pool,
            TransferType::Redemption,
            &id.to_string(),
            "tokens stranded at Alpaca, ticket 42",
        )
        .await
        .unwrap();

        let output = String::from_utf8(stdout).unwrap();
        assert_eq!(
            output,
            format!(
                "Redemption {id} marked as failed \
                 (reason: tokens stranded at Alpaca, ticket 42)\n"
            ),
        );

        let entity = st0x_event_sorcery::load_entity::<EquityRedemption>(&pool, &id)
            .await
            .unwrap()
            .expect("force-failed redemption must still materialize");
        let EquityRedemption::Failed { reason, .. } = entity else {
            panic!("force-failed redemption must replay to Failed, got {entity:?}");
        };
        assert_eq!(
            reason.as_deref(),
            Some("tokens stranded at Alpaca, ticket 42"),
            "operator reason must survive into the replayed Failed state",
        );
    }

    /// `transfer fail --kind redemption` on a redemption stuck in `Pending`
    /// (Alpaca detected the transfer but never completed it) must dispatch
    /// `RejectRedemption` and persist the operator's `--reason`.
    #[tokio::test]
    async fn fail_transfer_redemption_pending_rejects_with_reason() {
        let pool = setup_test_db().await;
        let id = redemption_aggregate_id("cli-stuck-pending");

        seed_redemption_to_tokens_sent(&pool, &id).await;
        send_redemption_command(
            &pool,
            &id,
            EquityRedemptionCommand::Detect {
                tokenization_request_id: tokenization_request_id("tok-cli-test"),
            },
        )
        .await;

        let mut stdout = Vec::new();
        fail_transfer_command(
            &mut stdout,
            &pool,
            TransferType::Redemption,
            &id.to_string(),
            "alpaca never completed, ticket 43",
        )
        .await
        .unwrap();

        let output = String::from_utf8(stdout).unwrap();
        assert_eq!(
            output,
            format!(
                "Redemption {id} marked as failed \
                 (reason: alpaca never completed, ticket 43)\n"
            ),
        );

        let entity = st0x_event_sorcery::load_entity::<EquityRedemption>(&pool, &id)
            .await
            .unwrap()
            .expect("rejected redemption must still materialize");
        let EquityRedemption::Failed { reason, .. } = entity else {
            panic!("rejected redemption must replay to Failed, got {entity:?}");
        };
        assert_eq!(
            reason.as_deref(),
            Some("alpaca never completed, ticket 43"),
            "rejection reason must survive into the replayed Failed state",
        );
    }

    /// A redemption stuck before the tokens leave the bot's custody takes the
    /// `FailTransfer` arm of the dispatch.
    #[tokio::test]
    async fn fail_transfer_redemption_early_state_fails_with_reason() {
        let pool = setup_test_db().await;
        let id = redemption_aggregate_id("cli-stuck-early");
        seed_redemption_to_withdrawn(&pool, &id).await;

        let mut stdout = Vec::new();
        fail_transfer_command(
            &mut stdout,
            &pool,
            TransferType::Redemption,
            &id.to_string(),
            "withdraw orphaned, ticket 44",
        )
        .await
        .unwrap();

        let output = String::from_utf8(stdout).unwrap();
        assert_eq!(
            output,
            format!(
                "Redemption {id} marked as failed \
                 (reason: withdraw orphaned, ticket 44)\n"
            ),
        );

        let entity = st0x_event_sorcery::load_entity::<EquityRedemption>(&pool, &id)
            .await
            .unwrap()
            .expect("transfer-failed redemption must still materialize");
        let EquityRedemption::Failed { reason, .. } = entity else {
            panic!("transfer-failed redemption must replay to Failed, got {entity:?}");
        };
        assert_eq!(
            reason.as_deref(),
            Some("withdraw orphaned, ticket 44"),
            "transfer-fail reason must survive into the replayed Failed state",
        );
    }

    /// A completed redemption must be refused, not force-failed.
    #[tokio::test]
    async fn fail_transfer_redemption_refuses_already_completed() {
        let pool = setup_test_db().await;
        let id = redemption_aggregate_id("cli-already-completed");

        seed_redemption_to_tokens_sent(&pool, &id).await;
        send_redemption_command(
            &pool,
            &id,
            EquityRedemptionCommand::Detect {
                tokenization_request_id: tokenization_request_id("tok-done"),
            },
        )
        .await;
        send_redemption_command(&pool, &id, EquityRedemptionCommand::Complete).await;

        let mut stdout = Vec::new();
        let result = fail_transfer_command(
            &mut stdout,
            &pool,
            TransferType::Redemption,
            &id.to_string(),
            "should be refused",
        )
        .await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("already completed"),
            "failing a completed redemption must refuse; got: {err_msg}"
        );
    }

    /// A typed command rejection (the TOCTOU case: the bot advanced the
    /// aggregate between the CLI's read and its write) gets the operator
    /// re-run hint, with the original error preserved in the chain.
    #[test]
    fn stale_state_context_adds_rerun_hint_for_command_rejections() {
        let error: AggregateError<LifecycleError<EquityRedemption>> =
            AggregateError::UserError(LifecycleError::Apply(EquityRedemptionError::AlreadyFailed));

        let wrapped = stale_state_context("Redemption", "some-id", error);

        let message = format!("{wrapped:#}");
        assert!(
            message.contains("Redemption some-id rejected the failure command"),
            "got: {message}"
        );
        assert!(
            message.contains("re-run to see the current state"),
            "got: {message}"
        );
        assert!(
            wrapped.source().is_some(),
            "the original aggregate error must be preserved as the source"
        );
    }

    /// Infrastructure failures must pass through untouched -- the stale-state
    /// hint would misattribute a database problem to a concurrency race.
    #[test]
    fn stale_state_context_passes_infrastructure_errors_through() {
        let error: AggregateError<LifecycleError<EquityRedemption>> =
            AggregateError::UnexpectedError(Box::new(std::io::Error::other("db on fire")));

        let wrapped = stale_state_context("Redemption", "some-id", error);

        let message = format!("{wrapped:#}");
        assert!(
            !message.contains("re-run"),
            "infrastructure errors must not get the stale-state hint; got: {message}"
        );
        assert!(message.contains("db on fire"), "got: {message}");
    }

    /// Drives a redemption through the real command flow to its terminal
    /// `Reconciled` state: stuck in `TokensSent`, force-failed, then reconciled.
    async fn seed_redemption_to_reconciled(pool: &SqlitePool, id: &RedemptionAggregateId) {
        seed_redemption_to_tokens_sent(pool, id).await;
        send_redemption_command(
            pool,
            id,
            EquityRedemptionCommand::FailDetection {
                failure: DetectionFailure::Timeout,
            },
        )
        .await;
        send_redemption_command(
            pool,
            id,
            EquityRedemptionCommand::Reconcile {
                reason: "deposited manually via vault-deposit".to_string(),
            },
        )
        .await;
    }

    /// Drives a mint through the real command flow to its `Failed` terminal:
    /// requested, then acceptance force-failed. Uses `()` services because
    /// `TokenizedEquityMint` performs no I/O in its handlers.
    async fn seed_mint_to_failed(pool: &SqlitePool, id: &IssuerRequestId) {
        let store = StoreBuilder::<TokenizedEquityMint>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        store
            .send(
                id,
                TokenizedEquityMintCommand::RecordMintRequested {
                    issuer_request_id: id.clone(),
                    symbol: Symbol::new("AAPL").unwrap(),
                    quantity: float!(10),
                    wallet: Address::ZERO,
                    tokenization_request_id: tokenization_request_id("test-req-id"),
                },
            )
            .await
            .unwrap();
        store
            .send(
                id,
                TokenizedEquityMintCommand::FailAcceptance {
                    reason: "seed: timed out".to_string(),
                },
            )
            .await
            .unwrap();
    }

    async fn send_mint_command(
        pool: &SqlitePool,
        id: &IssuerRequestId,
        command: TokenizedEquityMintCommand,
    ) {
        let store = StoreBuilder::<TokenizedEquityMint>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        store.send(id, command).await.unwrap();
    }

    #[tokio::test]
    async fn reconcile_equity_mint_rejects_unknown_id() {
        let pool = setup_test_db().await;

        let mut stdout = Vec::new();
        let result = reconcile_equity_transfer_command(
            &mut stdout,
            TransferType::Mint,
            &issuer_request_id("cli-no-such-mint").to_string(),
            "handled out-of-band".to_string(),
            &pool,
        )
        .await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("not found"),
            "reconcile of an unknown mint must refuse; got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn reconcile_equity_mint_rejects_non_failed() {
        let pool = setup_test_db().await;
        let id = issuer_request_id("cli-mint-reconcile-non-failed");
        send_mint_command(
            &pool,
            &id,
            TokenizedEquityMintCommand::RecordMintRequested {
                issuer_request_id: id.clone(),
                symbol: Symbol::new("AAPL").unwrap(),
                quantity: float!(10),
                wallet: Address::ZERO,
                tokenization_request_id: tokenization_request_id("test-req-id"),
            },
        )
        .await;

        let mut stdout = Vec::new();
        let result = reconcile_equity_transfer_command(
            &mut stdout,
            TransferType::Mint,
            &id.to_string(),
            "handled out-of-band".to_string(),
            &pool,
        )
        .await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("not in the Failed state"),
            "reconcile of a non-failed mint must refuse; got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn reconcile_equity_mint_succeeds_from_failed() {
        let pool = setup_test_db().await;
        let id = issuer_request_id("cli-mint-reconcile-from-failed");
        seed_mint_to_failed(&pool, &id).await;

        let mut stdout = Vec::new();
        reconcile_equity_transfer_command(
            &mut stdout,
            TransferType::Mint,
            &id.to_string(),
            "wrapped manually via wrap-equity".to_string(),
            &pool,
        )
        .await
        .unwrap();

        let entity = st0x_event_sorcery::load_entity::<TokenizedEquityMint>(&pool, &id)
            .await
            .unwrap()
            .unwrap();
        let TokenizedEquityMint::Reconciled {
            reconcile_reason, ..
        } = entity
        else {
            panic!("a reconciled mint must reach the Reconciled terminal, got: {entity:?}");
        };
        assert_eq!(reconcile_reason, "wrapped manually via wrap-equity");
    }

    #[tokio::test]
    async fn reconcile_equity_mint_refuses_double_reconcile() {
        let pool = setup_test_db().await;
        let id = issuer_request_id("cli-mint-double-reconcile");
        seed_mint_to_failed(&pool, &id).await;
        send_mint_command(
            &pool,
            &id,
            TokenizedEquityMintCommand::Reconcile {
                reason: "wrapped manually via wrap-equity".to_string(),
            },
        )
        .await;

        let mut stdout = Vec::new();
        let result = reconcile_equity_transfer_command(
            &mut stdout,
            TransferType::Mint,
            &id.to_string(),
            "second reconcile attempt".to_string(),
            &pool,
        )
        .await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("not in the Failed state"),
            "a second reconcile of an already-reconciled mint must refuse; got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn fail_transfer_mint_refuses_already_reconciled() {
        let pool = setup_test_db().await;
        let id = issuer_request_id("cli-mint-already-reconciled");
        seed_mint_to_failed(&pool, &id).await;
        send_mint_command(
            &pool,
            &id,
            TokenizedEquityMintCommand::Reconcile {
                reason: "wrapped manually via wrap-equity".to_string(),
            },
        )
        .await;

        let mut stdout = Vec::new();
        let result = fail_transfer_command(
            &mut stdout,
            &pool,
            TransferType::Mint,
            &id.to_string(),
            "should be refused",
        )
        .await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("already reconciled"),
            "failing an already-reconciled mint must refuse; got: {err_msg}"
        );
    }

    /// Inserts a raw event row for a `TokenizedEquityMint` aggregate. The
    /// `MintRequested`-only state is unreachable via commands by design (the bot
    /// requests and accepts/rejects in one atomic command), so a mint genuinely
    /// stuck pre-acceptance only exists in an abnormal event log and must be
    /// seeded as a raw event -- the established test-only pattern for states
    /// unreachable via commands, mirroring this aggregate's own test module.
    /// Sequences are 1-based (the snapshot-aware loader reads `sequence > 0`).
    async fn insert_mint_event(
        pool: &SqlitePool,
        id: &IssuerRequestId,
        sequence: i64,
        event_type: &str,
        payload: &str,
    ) {
        sqlx::query(
            "INSERT INTO events \
             (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata) \
             VALUES ('TokenizedEquityMint', ?, ?, ?, '1.0', ?, '{}')",
        )
        .bind(id.to_string())
        .bind(sequence)
        .bind(event_type)
        .bind(payload)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn fail_transfer_mint_force_fails_from_requested() {
        // RAI-999: a mint stuck at MintRequested (requested at the provider but
        // never accepted) must force-fail via the CLI -- previously the CLI
        // bailed with "cannot fail before acceptance". The state is unreachable
        // via commands, so seed a bare MintRequested raw event.
        let pool = setup_test_db().await;
        let id = issuer_request_id("cli-mint-requested");
        insert_mint_event(
            &pool,
            &id,
            1,
            "TokenizedEquityMintEvent::MintRequested",
            r#"{"MintRequested":{"symbol":"AAPL","quantity":"10","wallet":"0x0000000000000000000000000000000000000001","requested_at":"2026-01-01T00:00:00Z"}}"#,
        )
        .await;

        // Guard: the precondition must actually be MintRequested, so the test
        // cannot silently drift to exercising a different dispatch arm.
        let seeded = st0x_event_sorcery::load_entity::<TokenizedEquityMint>(&pool, &id)
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(seeded, TokenizedEquityMint::MintRequested { .. }),
            "precondition must be MintRequested, got: {seeded:?}"
        );

        let mut stdout = Vec::new();
        fail_transfer_command(
            &mut stdout,
            &pool,
            TransferType::Mint,
            &id.to_string(),
            "stuck at provider, never accepted",
        )
        .await
        .unwrap();

        assert_eq!(
            String::from_utf8(stdout).unwrap(),
            format!("Mint {id} marked as failed\n"),
            "the operator-facing confirmation message must be printed"
        );

        let entity = st0x_event_sorcery::load_entity::<TokenizedEquityMint>(&pool, &id)
            .await
            .unwrap()
            .unwrap();
        let TokenizedEquityMint::Failed { reason, .. } = entity else {
            panic!(
                "a force-failed MintRequested mint must reach the Failed terminal, got: {entity:?}"
            );
        };
        assert_eq!(
            reason, "stuck at provider, never accepted",
            "the operator reason must survive the full CLI dispatch into the Failed state"
        );
    }

    #[tokio::test]
    async fn reconcile_equity_redemption_rejects_unknown_id() {
        let pool = setup_test_db().await;

        let mut stdout = Vec::new();
        let result = reconcile_equity_transfer_command(
            &mut stdout,
            TransferType::Redemption,
            &redemption_aggregate_id("cli-no-such-redemption").to_string(),
            "handled out-of-band".to_string(),
            &pool,
        )
        .await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("not found"),
            "reconcile of an unknown redemption must refuse; got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn reconcile_equity_redemption_rejects_non_failed() {
        let pool = setup_test_db().await;
        let id = redemption_aggregate_id("cli-reconcile-non-failed");
        seed_redemption_to_tokens_sent(&pool, &id).await;

        let mut stdout = Vec::new();
        let result = reconcile_equity_transfer_command(
            &mut stdout,
            TransferType::Redemption,
            &id.to_string(),
            "handled out-of-band".to_string(),
            &pool,
        )
        .await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("not in the Failed state"),
            "reconcile of a non-failed redemption must refuse; got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn reconcile_equity_redemption_succeeds_from_failed() {
        let pool = setup_test_db().await;
        let id = redemption_aggregate_id("cli-reconcile-from-failed");
        seed_redemption_to_tokens_sent(&pool, &id).await;
        send_redemption_command(
            &pool,
            &id,
            EquityRedemptionCommand::FailDetection {
                failure: DetectionFailure::Timeout,
            },
        )
        .await;

        let mut stdout = Vec::new();
        reconcile_equity_transfer_command(
            &mut stdout,
            TransferType::Redemption,
            &id.to_string(),
            "deposited manually via vault-deposit".to_string(),
            &pool,
        )
        .await
        .unwrap();

        let entity = st0x_event_sorcery::load_entity::<EquityRedemption>(&pool, &id)
            .await
            .unwrap()
            .unwrap();
        let EquityRedemption::Reconciled {
            reconcile_reason, ..
        } = entity
        else {
            panic!("a reconciled redemption must reach the Reconciled terminal, got: {entity:?}");
        };
        assert_eq!(reconcile_reason, "deposited manually via vault-deposit");

        let output = String::from_utf8(stdout).unwrap();
        assert!(
            output.contains("restart"),
            "redemption reconcile success line must warn a restart is still required; got: {output}"
        );
    }

    #[tokio::test]
    async fn reconcile_equity_redemption_refuses_double_reconcile() {
        let pool = setup_test_db().await;
        let id = redemption_aggregate_id("cli-redemption-double-reconcile");
        seed_redemption_to_reconciled(&pool, &id).await;

        let mut stdout = Vec::new();
        let result = reconcile_equity_transfer_command(
            &mut stdout,
            TransferType::Redemption,
            &id.to_string(),
            "second reconcile attempt".to_string(),
            &pool,
        )
        .await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("not in the Failed state"),
            "a second reconcile of an already-reconciled redemption must refuse; got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn fail_transfer_redemption_refuses_already_reconciled() {
        let pool = setup_test_db().await;
        let id = redemption_aggregate_id("cli-already-reconciled");
        seed_redemption_to_reconciled(&pool, &id).await;

        let mut stdout = Vec::new();
        let result = fail_transfer_command(
            &mut stdout,
            &pool,
            TransferType::Redemption,
            &id.to_string(),
            "should be refused",
        )
        .await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("already reconciled"),
            "failing an already-reconciled redemption must refuse; got: {err_msg}"
        );
    }

    /// A redemption already in a terminal state must be refused, not
    /// double-failed.
    #[tokio::test]
    async fn fail_transfer_redemption_refuses_already_failed() {
        let pool = setup_test_db().await;
        let id = redemption_aggregate_id("cli-already-failed");

        seed_redemption_to_tokens_sent(&pool, &id).await;
        send_redemption_command(
            &pool,
            &id,
            EquityRedemptionCommand::FailDetection {
                failure: DetectionFailure::Timeout,
            },
        )
        .await;

        let mut stdout = Vec::new();
        let result = fail_transfer_command(
            &mut stdout,
            &pool,
            TransferType::Redemption,
            &id.to_string(),
            "should be refused",
        )
        .await;

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("already failed"),
            "failing an already-failed redemption must refuse; got: {err_msg}"
        );
    }
}

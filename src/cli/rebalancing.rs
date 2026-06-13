//! Transfer equity and USDC rebalancing CLI commands.

use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use sqlx::SqlitePool;
use std::future::Future;
use std::io::{self, Write};
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;
use uuid::Uuid;

use st0x_bridge::cctp::{CctpBridge, CctpCtx};
use st0x_event_sorcery::{AggregateError, StoreBuilder};
use st0x_evm::{Evm, OpenChainErrorRegistry, ReadOnlyEvm};
use st0x_execution::{
    AlpacaBrokerApi, AlpacaBrokerApiCtx, AlpacaBrokerApiMode, Executor, FractionalShares, Symbol,
    TimeInForce,
};
use st0x_finance::Usdc;
use st0x_raindex::{RaindexService, RaindexVaultId};
use st0x_wrapper::{Wrapper, WrapperService};

use super::{TransferDirection, TransferType};
use crate::alpaca_wallet::AlpacaWalletService;
use crate::api::ResumeResponse;
use crate::bindings::IERC20;
use crate::equity_redemption::{
    DetectionFailure, EquityRedemption, EquityRedemptionCommand, RedemptionAggregateId,
};
use crate::onchain::{USDC_BASE, USDC_ETHEREUM};
use crate::rebalancing::equity::{CrossVenueEquityTransfer, Equity, EquityTransferServices};
use crate::rebalancing::to_wrapped_equities;
use crate::rebalancing::transfer::{CrossVenueTransfer, HedgingVenue, MarketMakingVenue};
use crate::rebalancing::usdc::CrossVenueCashTransfer;
use crate::rebalancing::usdc::UsdcTransferError;
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

            CrossVenueTransfer::<HedgingVenue, MarketMakingVenue>::transfer(
                &equity_transfer,
                Equity {
                    symbol: symbol.clone(),
                    quantity,
                },
            )
            .await?;

            writeln!(stdout, "✅ Mint completed successfully")?;
        }

        TransferDirection::ToAlpaca => {
            writeln!(stdout, "   Sending tokens for redemption...")?;

            CrossVenueTransfer::<MarketMakingVenue, HedgingVenue>::transfer(
                &equity_transfer,
                Equity {
                    symbol: symbol.clone(),
                    quantity,
                },
            )
            .await?;

            writeln!(stdout, "✅ Redemption completed successfully")?;
        }
    }

    Ok(())
}

/// Per-retry delay for the manual CLI redrive loop when Circle's attestation is
/// not yet ready. Mirrors the apalis job's redrive cadence.
const CLI_ATTESTATION_REDRIVE_DELAY: Duration = Duration::from_secs(60);

/// Drives a manual USDC transfer to a terminal outcome, redriving on attestation
/// timeouts so a single CLI invocation cannot strand after the burn.
///
/// `resume` is re-invoked with the same `UsdcRebalanceId` on every attempt, so a
/// timeout resumes the already-burned transfer instead of starting a second
/// fund-moving one. Any non-timeout error -- including
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
            Err(error) => return Err(error),
        }
    }
}

/// Starts a fresh manual USDC transfer. Surfaces the generated id up front so an
/// operator can resume it (via `transfer resume`) if the process is
/// interrupted, then drives it to terminal with attestation-timeout redrive.
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

    let attestation_retry_deadline = ctx.rebalancing_ctx()?.attestation_retry_deadline;

    let rebalance_manager = CrossVenueCashTransfer::new(
        alpaca_broker,
        alpaca_wallet,
        bridge,
        vault_service,
        usdc_store,
        owner,
        RaindexVaultId(usdc_vault_id),
        attestation_retry_deadline,
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
         and USDC rebalancing resumes."
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

    let issuer_request_id = IssuerRequestId::new(Uuid::new_v4().to_string());
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
    let services = EquityTransferServices::panicking();

    match transfer_type {
        TransferType::Mint => {
            let mint_id = IssuerRequestId::new(id);

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
            };

            st0x_event_sorcery::send_command::<EquityRedemption>(
                pool,
                &redemption_id,
                command,
                services,
            )
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
    use alloy::primitives::{Address, B256, address, b256};
    use rain_math_float::Float;
    use url::Url;
    use uuid::uuid;

    use st0x_config::{
        AssetsConfig, CashAssetConfig, EquitiesConfig, EvmCtx, ExecutionThreshold, LogLevel,
        OperationMode, RebalancingCtx, TradingMode,
    };
    use st0x_event_sorcery::LifecycleError;
    use st0x_execution::{
        AlpacaAccountId, AlpacaBrokerApiCtx, AlpacaBrokerApiMode, Symbol, TimeInForce,
    };
    use st0x_finance::Usdc;
    use st0x_float_macro::float;
    use st0x_wrapper::MockWrapper;

    use super::*;
    use crate::equity_redemption::EquityRedemptionError;
    use crate::inventory::ImbalanceThreshold;
    use crate::onchain::mock::MockRaindex;
    use crate::test_utils::setup_test_db;
    use crate::tokenization::mock::MockTokenizer;
    use crate::tokenized_equity_mint::TokenizationRequestId;
    use crate::usdc_rebalance::{ReconcileReason, TransferRef, UsdcRebalanceCommand};
    use crate::vault_lookup::MockVaultLookup;

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

    fn redemption_services() -> EquityTransferServices {
        EquityTransferServices {
            raindex: Arc::new(MockRaindex::new()),
            vault_lookup: Arc::new(
                MockVaultLookup::new().with_default_vault(RaindexVaultId(B256::ZERO)),
            ),
            tokenizer: Arc::new(MockTokenizer::new()),
            wrapper: Arc::new(MockWrapper::new()),
        }
    }

    /// Drives a redemption through the real CQRS command flow until the vault
    /// withdrawal is confirmed (`WithdrawnFromRaindex`) -- stuck before the
    /// tokens leave the bot's custody.
    async fn seed_redemption_to_withdrawn(pool: &SqlitePool, id: &RedemptionAggregateId) {
        use EquityRedemptionCommand::*;

        let store = StoreBuilder::<EquityRedemption>::new(pool.clone())
            .build(redemption_services())
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
        store.send(id, SubmitWithdraw).await.unwrap();
        store.send(id, ConfirmWithdraw).await.unwrap();
    }

    /// Continues the real CQRS command flow until the redemption is stuck in
    /// `TokensSent`: tokens reached Alpaca's redemption wallet but detection
    /// never fired.
    async fn seed_redemption_to_tokens_sent(pool: &SqlitePool, id: &RedemptionAggregateId) {
        use EquityRedemptionCommand::*;

        seed_redemption_to_withdrawn(pool, id).await;

        let store = StoreBuilder::<EquityRedemption>::new(pool.clone())
            .build(redemption_services())
            .await
            .unwrap();
        for command in [
            UnwrapTokens,
            SubmitUnwrap,
            ConfirmUnwrap,
            PrepareSend,
            SendTokens,
        ] {
            store.send(id, command).await.unwrap();
        }
    }

    async fn send_redemption_command(
        pool: &SqlitePool,
        id: &RedemptionAggregateId,
        command: EquityRedemptionCommand,
    ) {
        let store = StoreBuilder::<EquityRedemption>::new(pool.clone())
            .build(redemption_services())
            .await
            .unwrap();
        store.send(id, command).await.unwrap();
    }

    /// `fail-transfer --type redemption` on a redemption stuck in `TokensSent`
    /// must dispatch `FailDetection { Operator }` and persist the operator's
    /// `--reason`, recoverable from the replayed `Failed` state.
    #[tokio::test]
    async fn fail_transfer_redemption_tokens_sent_persists_operator_reason() {
        let pool = setup_test_db().await;
        let id = RedemptionAggregateId("cli-stuck-tokens-sent".to_string());
        seed_redemption_to_tokens_sent(&pool, &id).await;

        let mut stdout = Vec::new();
        fail_transfer_command(
            &mut stdout,
            &pool,
            TransferType::Redemption,
            "cli-stuck-tokens-sent",
            "tokens stranded at Alpaca, ticket 42",
        )
        .await
        .unwrap();

        let output = String::from_utf8(stdout).unwrap();
        assert_eq!(
            output,
            "Redemption cli-stuck-tokens-sent marked as failed \
             (reason: tokens stranded at Alpaca, ticket 42)\n",
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

    /// `fail-transfer --type redemption` on a redemption stuck in `Pending`
    /// (Alpaca detected the transfer but never completed it) must dispatch
    /// `RejectRedemption` and persist the operator's `--reason`.
    #[tokio::test]
    async fn fail_transfer_redemption_pending_rejects_with_reason() {
        let pool = setup_test_db().await;
        let id = RedemptionAggregateId("cli-stuck-pending".to_string());

        seed_redemption_to_tokens_sent(&pool, &id).await;
        send_redemption_command(
            &pool,
            &id,
            EquityRedemptionCommand::Detect {
                tokenization_request_id: TokenizationRequestId("tok-cli-test".to_string()),
            },
        )
        .await;

        let mut stdout = Vec::new();
        fail_transfer_command(
            &mut stdout,
            &pool,
            TransferType::Redemption,
            "cli-stuck-pending",
            "alpaca never completed, ticket 43",
        )
        .await
        .unwrap();

        let output = String::from_utf8(stdout).unwrap();
        assert_eq!(
            output,
            "Redemption cli-stuck-pending marked as failed \
             (reason: alpaca never completed, ticket 43)\n",
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
        let id = RedemptionAggregateId("cli-stuck-early".to_string());
        seed_redemption_to_withdrawn(&pool, &id).await;

        let mut stdout = Vec::new();
        fail_transfer_command(
            &mut stdout,
            &pool,
            TransferType::Redemption,
            "cli-stuck-early",
            "withdraw orphaned, ticket 44",
        )
        .await
        .unwrap();

        let output = String::from_utf8(stdout).unwrap();
        assert_eq!(
            output,
            "Redemption cli-stuck-early marked as failed \
             (reason: withdraw orphaned, ticket 44)\n",
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
        let id = RedemptionAggregateId("cli-already-completed".to_string());

        seed_redemption_to_tokens_sent(&pool, &id).await;
        send_redemption_command(
            &pool,
            &id,
            EquityRedemptionCommand::Detect {
                tokenization_request_id: TokenizationRequestId("tok-done".to_string()),
            },
        )
        .await;
        send_redemption_command(&pool, &id, EquityRedemptionCommand::Complete).await;

        let mut stdout = Vec::new();
        let result = fail_transfer_command(
            &mut stdout,
            &pool,
            TransferType::Redemption,
            "cli-already-completed",
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

    /// A redemption already in a terminal state must be refused, not
    /// double-failed.
    #[tokio::test]
    async fn fail_transfer_redemption_refuses_already_failed() {
        let pool = setup_test_db().await;
        let id = RedemptionAggregateId("cli-already-failed".to_string());

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
            "cli-already-failed",
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

//! The Relay hop of a cash transfer: the corridor chain's stable deposited
//! into Relay's depository, paid out by Relay's solver at the Ethereum hub.
//!
//! This build runs the chain-to-Alpaca side up to the confirmed deposit:
//! a pre-flight quote, the vault withdraw, the binding quote (`SwapQuoted`),
//! the approve and deposit signed and persisted together
//! (`SwapDepositPrepared`), their broadcast, and the deposit confirmed to
//! the origin chain's depth (`SwapDeposited`). The transfer then waits there
//! with its guard held.

use alloy::primitives::{Address, B256, TxHash, U256};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::SqlitePool;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::Mutex;
use tokio::task::{JoinError, JoinHandle};
use tokio::time::Instant;
use tracing::{error, info, warn};

use st0x_bridge::cctp::{AttestationResponse, CctpError, UsdcTransferStatus};
use st0x_bridge::corridor::{HopKind, UsdcCorridor};
use st0x_bridge::relay::{
    BasisPoints, QuoteAmounts, QuoteBounds, QuoteFees, QuoteRequest, RelayBridge, RelayBridgeError,
    RelayClient, RelayOrderId, RelayQuote, RelayRequestId, StepTransaction,
};
use st0x_bridge::{BridgeDirection, HopDirection, PreparedSwap, PreparedSwapDeposit, SwapBridge};
use st0x_config::RelayHopCtx;
use st0x_event_sorcery::Store;
use st0x_evm::{Chain, MinedTx, PreparedTransaction, Wallet};
use st0x_finance::Usdc;

use super::manager::{RecoveredCctpMint, UsdcBridgeHelper, usdc_to_u256};
use super::{
    CctpMintRecoveryError, CrossVenueCashTransfer, DepositSendNotSuperseded, RecheckUsdcDeposit,
    RecoverCctpMint, RestorePreparedDepositSends, RestoredDepositSends, ResumeAlpacaToBase,
    ResumeBaseToAlpaca, UsdcRecheckError, UsdcTransferError,
};
use crate::native_gas::TransferGasRoute;
use crate::rebalancing::equity::RecheckOutcome;
use crate::usdc_rebalance::{
    RebalanceDirection, SwapQuote, SwapStep, UsdcRebalance, UsdcRebalanceCommand, UsdcRebalanceId,
    prepared_swap_ids,
};

/// One Relay corridor's hop: the bridge that signs the deposit and proves
/// the payment, the API client that quotes, the corridor's bounds, and the
/// lock that serializes the signing of a pair on the corridor chain's wallet.
pub(crate) struct RelayHop<Signer> {
    bridge: RelayBridge<Signer, Signer>,
    client: RelayClient,
    bounds: RelayHopCtx,
    /// Where the solver pays: our wallet at the Ethereum hub.
    hub_wallet: Address,
    chain_send_prepare: Mutex<()>,
}

impl<Signer> RelayHop<Signer> {
    pub(crate) fn new(
        bridge: RelayBridge<Signer, Signer>,
        client: RelayClient,
        bounds: RelayHopCtx,
        hub_wallet: Address,
    ) -> Self {
        Self {
            bridge,
            client,
            bounds,
            hub_wallet,
            chain_send_prepare: Mutex::new(()),
        }
    }
}

/// The chain-to-Alpaca side of a Relay hop: the corridor chain is the origin.
const TO_HUB: HopDirection = HopDirection::ToHub;

impl<Signer: Wallet> RelayHop<Signer> {
    /// Signs and persists the pair, or picks up the one already persisted;
    /// `None` once the deposit is confirmed. Read and signed under the chain
    /// wallet's prepare lock: a timed-out attempt's prepare may have
    /// persisted a pair this one must send instead of signing anew. The RPC
    /// work under the lock is bounded by `quote_max_age`, past which the
    /// quote is stale anyway, so a hung RPC cannot hold the lock forever.
    async fn prepare_swap_pair(
        self: &Arc<Self>,
        cqrs: &Store<UsdcRebalance>,
        id: &UsdcRebalanceId,
    ) -> Result<Option<(PreparedSwapDeposit, B256)>, UsdcTransferError> {
        let _prepare = self.chain_send_prepare.lock().await;
        let deadline = Instant::now() + self.bounds.quote_max_age;

        match cqrs.load(id).await? {
            Some(UsdcRebalance::SwapQuoted {
                quote,
                split_approves,
                quoted_at,
                ..
            }) => {
                let pair = self
                    .sign_swap_pair(cqrs, id, &quote, quoted_at, &split_approves, deadline)
                    .await?;
                Ok(Some((pair, quote.order_id)))
            }
            Some(UsdcRebalance::SwapDepositPrepared {
                quote,
                split_approves,
                approve,
                deposit,
                ..
            }) => {
                self.broadcast_split_approves(id, &split_approves, deadline)
                    .await?;
                Ok(Some((
                    PreparedSwapDeposit { approve, deposit },
                    quote.order_id,
                )))
            }
            Some(UsdcRebalance::SwapDeposited { .. }) => Ok(None),
            None => Err(UsdcTransferError::StateOffHop {
                id: id.clone(),
                state: "Uninitialized",
                hop: HopKind::Relay,
            }),
            Some(
                state @ (UsdcRebalance::Converting { .. }
                | UsdcRebalance::ConversionComplete { .. }
                | UsdcRebalance::ConversionFailed { .. }
                | UsdcRebalance::WithdrawalSubmitting { .. }
                | UsdcRebalance::Withdrawing { .. }
                | UsdcRebalance::WithdrawalComplete { .. }
                | UsdcRebalance::WithdrawalFailed { .. }
                | UsdcRebalance::BridgingSubmitting { .. }
                | UsdcRebalance::Bridging { .. }
                | UsdcRebalance::AwaitingAttestation { .. }
                | UsdcRebalance::Attested { .. }
                | UsdcRebalance::Bridged { .. }
                | UsdcRebalance::BridgingFailed { .. }
                | UsdcRebalance::DepositInitiated { .. }
                | UsdcRebalance::DepositConfirmed { .. }
                | UsdcRebalance::DepositFailed { .. }
                | UsdcRebalance::Reconciled { .. }),
            ) => Err(UsdcTransferError::StateOffHop {
                id: id.clone(),
                state: state.state_name(),
                hop: HopKind::Relay,
            }),
        }
    }

    /// Signs the approve and deposit and persists both before either is
    /// broadcast. Approves that went out alone are sent again first, so the
    /// pair is not signed behind a nonce no node holds. An expired quote is
    /// not signed.
    async fn sign_swap_pair(
        self: &Arc<Self>,
        cqrs: &Store<UsdcRebalance>,
        id: &UsdcRebalanceId,
        quote: &SwapQuote,
        quoted_at: DateTime<Utc>,
        split_approves: &[PreparedTransaction],
        deadline: Instant,
    ) -> Result<PreparedSwapDeposit, UsdcTransferError> {
        self.broadcast_split_approves(id, split_approves, deadline)
            .await?;

        // A quote resumed after downtime or retries may be stale. Nothing
        // re-quotes a recorded quote yet, so the transfer stays at
        // `SwapQuoted` with its guard held until a re-quote or a redeposit
        // is built to move it on.
        if quote_expired(quote, quoted_at, self.bounds.quote_max_age, Utc::now()) {
            warn!(target: "rebalance", %id, %quoted_at, deadline = %quote.deadline, max_age = ?self.bounds.quote_max_age, "Relay quote expired before its deposit was signed; not signing, the transfer holds its guard at SwapQuoted");
            return Err(UsdcTransferError::SwapQuoteExpired {
                id: id.clone(),
                quoted_at,
                deadline: quote.deadline,
            });
        }

        // After a split the allowance may already cover the deposit: the
        // bridge reads it and signs an approve only when it falls short.
        let relay_quote = relay_quote(quote, split_approves.is_empty())?;
        let prepared = self.sign_before(id, relay_quote, deadline).await?;

        match prepared {
            PreparedSwap::Deposit(pair) => {
                self.persist_swap_pair(cqrs, id, &pair).await?;
                Ok(pair)
            }
            PreparedSwap::ApproveOnly { approve } => {
                self.persist_lone_approve(cqrs, id, &approve).await?;
                // Persisted: a retry sends it again if this times out.
                self.before(
                    id,
                    deadline,
                    self.bridge.broadcast_approve(TO_HUB, &approve),
                )
                .await?
                .map_err(Box::new)?;
                warn!(target: "rebalance", %id, approve = %approve.tx_hash(), "Another send split the Relay pair; its approve went out alone");
                Err(UsdcTransferError::SwapPairSplit { id: id.clone() })
            }
        }
    }

    /// Signs the pair on a task of its own, so a timeout does not drop a
    /// signing that holds nonces: a pair signed after `deadline` has its
    /// nonces released, as an unpersisted pair's are, once it returns.
    async fn sign_before(
        self: &Arc<Self>,
        id: &UsdcRebalanceId,
        quote: RelayQuote,
        deadline: Instant,
    ) -> Result<PreparedSwap, UsdcTransferError> {
        let hop = Arc::clone(self);
        let mut signing =
            tokio::spawn(async move { hop.bridge.prepare_deposit(TO_HUB, &quote).await });

        match tokio::time::timeout_at(deadline, &mut signing).await {
            Ok(Ok(prepared)) => Ok(prepared.map_err(Box::new)?),
            Ok(Err(join_error)) => Err(prepare_panicked(id, &join_error)),
            Err(_elapsed) => {
                let hop = Arc::clone(self);
                let late_id = id.clone();
                tokio::spawn(async move { hop.discard_late_signing(&late_id, signing).await });
                Err(self.prepare_timed_out(id))
            }
        }
    }

    /// Releases the nonces of a pair signed after its attempt timed out.
    async fn discard_late_signing(
        &self,
        id: &UsdcRebalanceId,
        signing: JoinHandle<Result<PreparedSwap, RelayBridgeError>>,
    ) {
        match signing.await {
            Ok(Ok(prepared)) => {
                warn!(target: "rebalance", %id, "Releasing the nonces of a Relay pair signed after its prepare timed out");
                self.bridge.discard_prepared(TO_HUB, &prepared).await;
            }
            Ok(Err(error)) => {
                warn!(target: "rebalance", %id, ?error, "A timed-out Relay pair signing failed; it holds no nonce");
            }
            Err(join_error) => {
                prepare_panicked(id, &join_error);
            }
        }
    }

    /// `work`'s output, or [`UsdcTransferError::SwapPrepareTimedOut`] once
    /// `deadline` passes.
    async fn before<Output>(
        &self,
        id: &UsdcRebalanceId,
        deadline: Instant,
        work: impl Future<Output = Output>,
    ) -> Result<Output, UsdcTransferError> {
        tokio::time::timeout_at(deadline, work)
            .await
            .map_err(|_elapsed| self.prepare_timed_out(id))
    }

    fn prepare_timed_out(&self, id: &UsdcRebalanceId) -> UsdcTransferError {
        let timeout = self.bounds.quote_max_age;
        warn!(target: "rebalance", %id, ?timeout, "Relay pair prepare timed out; releasing the chain wallet's prepare lock");
        UsdcTransferError::SwapPrepareTimedOut {
            id: id.clone(),
            timeout,
        }
    }

    /// Sends again, in nonce order, the approves that went out alone: the
    /// pair's nonces follow theirs, and a node may have dropped them. Each
    /// is persisted, so a timeout leaves it for the retry.
    async fn broadcast_split_approves(
        &self,
        id: &UsdcRebalanceId,
        split_approves: &[PreparedTransaction],
        deadline: Instant,
    ) -> Result<(), UsdcTransferError> {
        self.before(id, deadline, async {
            for approve in split_approves {
                self.bridge
                    .broadcast_approve(TO_HUB, approve)
                    .await
                    .map_err(Box::new)?;
            }

            Ok(())
        })
        .await?
    }

    /// Persists the signed pair. When the write fails, the nonces are
    /// released only if a reload proves the pair was not persisted.
    async fn persist_swap_pair(
        &self,
        cqrs: &Store<UsdcRebalance>,
        id: &UsdcRebalanceId,
        pair: &PreparedSwapDeposit,
    ) -> Result<(), UsdcTransferError> {
        let Err(error) = cqrs
            .send(
                id,
                UsdcRebalanceCommand::PrepareSwapDeposit {
                    approve: pair.approve.clone(),
                    deposit: pair.deposit.clone(),
                },
            )
            .await
        else {
            return Ok(());
        };

        match cqrs.load(id).await {
            Ok(Some(UsdcRebalance::SwapDepositPrepared { deposit, .. }))
                if deposit.tx_hash() == pair.deposit.tx_hash() =>
            {
                warn!(target: "rebalance", %id, ?error, "The failed Relay pair write committed; sending it");
                Ok(())
            }
            Ok(Some(UsdcRebalance::SwapQuoted { .. })) => {
                warn!(target: "rebalance", %id, ?error, "Releasing the nonces of a Relay pair that was not persisted");
                self.bridge
                    .discard_prepared(TO_HUB, &PreparedSwap::Deposit(pair.clone()))
                    .await;
                Err(error.into())
            }
            reload => {
                let reload = reload.map(|state| state.map(|state| state.state_name()));
                error!(target: "operational_alert", alert = true, %id, deposit = %pair.deposit.tx_hash(), ?reload, "Cannot tell whether a signed Relay pair was persisted; its nonces stay reserved and later sends from the chain wallet wait behind them until a restart");
                Err(error.into())
            }
        }
    }

    /// Persists an approve signed alone. Its nonce is released only if a
    /// reload proves it was not persisted.
    async fn persist_lone_approve(
        &self,
        cqrs: &Store<UsdcRebalance>,
        id: &UsdcRebalanceId,
        approve: &PreparedTransaction,
    ) -> Result<(), UsdcTransferError> {
        let Err(error) = cqrs
            .send(
                id,
                UsdcRebalanceCommand::PrepareSwapApprove {
                    approve: approve.clone(),
                },
            )
            .await
        else {
            return Ok(());
        };

        match cqrs.load(id).await {
            Ok(Some(UsdcRebalance::SwapQuoted { split_approves, .. }))
                if split_approves.contains(approve) =>
            {
                Ok(())
            }
            Ok(Some(UsdcRebalance::SwapQuoted { .. })) => {
                self.bridge
                    .discard_prepared(
                        TO_HUB,
                        &PreparedSwap::ApproveOnly {
                            approve: approve.clone(),
                        },
                    )
                    .await;
                Err(error.into())
            }
            reload => {
                let reload = reload.map(|state| state.map(|state| state.state_name()));
                error!(target: "operational_alert", alert = true, %id, approve = %approve.tx_hash(), ?reload, "Cannot tell whether a lone Relay approve was persisted; its nonce stays reserved until a restart");
                Err(error.into())
            }
        }
    }
}

impl<Signer> CrossVenueCashTransfer<Signer, RelayHop<Signer>>
where
    Signer: Wallet + Send + Sync + 'static,
{
    /// Drives a chain-to-Alpaca transfer on a Relay corridor from its
    /// recorded state. A state the Relay hop never reaches is refused.
    pub(crate) async fn resume_chain_to_alpaca_via_relay(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
        corridor: UsdcCorridor,
    ) -> Result<(), UsdcTransferError> {
        let state = self.cqrs.load(id).await?;
        self.require_served_corridor(id, corridor, state.as_ref())?;

        info!(target: "rebalance", ?state, "Resuming chain->Alpaca transfer over Relay");

        match state {
            None => self.execute_chain_to_alpaca_via_relay(id, amount).await,

            Some(UsdcRebalance::WithdrawalSubmitting {
                direction,
                amount,
                from_block,
                initiated_at,
                ..
            }) => {
                Self::require_base_to_alpaca(id, direction)?;
                let amount_u256 = usdc_to_u256(amount)?;
                self.resume_withdrawal_submitting(
                    id,
                    amount,
                    amount_u256,
                    from_block,
                    initiated_at,
                )
                .await?;
                self.quote_swap_after_withdrawal(id, amount).await
            }

            Some(UsdcRebalance::Withdrawing {
                direction, amount, ..
            }) => {
                Self::require_base_to_alpaca(id, direction)?;
                self.cqrs
                    .send(
                        id,
                        UsdcRebalanceCommand::ConfirmWithdrawal {
                            withdrawal_tx: None,
                        },
                    )
                    .await?;
                self.quote_swap_after_withdrawal(id, amount).await
            }

            Some(UsdcRebalance::WithdrawalComplete {
                direction, amount, ..
            }) => {
                Self::require_base_to_alpaca(id, direction)?;
                self.quote_swap_after_withdrawal(id, amount).await
            }

            Some(
                UsdcRebalance::SwapQuoted { direction, .. }
                | UsdcRebalance::SwapDepositPrepared { direction, .. },
            ) => {
                Self::require_base_to_alpaca(id, direction)?;
                self.send_swap_deposit(id).await
            }

            Some(UsdcRebalance::SwapDeposited {
                direction,
                deposit_tx,
                ..
            }) => {
                Self::require_base_to_alpaca(id, direction)?;
                warn!(
                    target: "rebalance",
                    %id,
                    %deposit_tx,
                    "Relay deposit confirmed; this build does not wait for the fill, so the \
                     transfer holds its guard at SwapDeposited"
                );
                Ok(())
            }

            Some(
                state @ (UsdcRebalance::Bridged { .. }
                | UsdcRebalance::DepositInitiated { .. }
                | UsdcRebalance::DepositConfirmed { .. }
                | UsdcRebalance::Converting { .. }
                | UsdcRebalance::ConversionComplete { .. }),
            ) => self.resume_base_to_alpaca_past_hop(id, state).await,

            Some(
                UsdcRebalance::WithdrawalFailed { .. }
                | UsdcRebalance::BridgingFailed { .. }
                | UsdcRebalance::DepositFailed { .. }
                | UsdcRebalance::ConversionFailed { .. },
            ) => Err(UsdcTransferError::PreviouslyFailedAggregate { id: id.clone() }),

            Some(UsdcRebalance::Reconciled { .. }) => Ok(()),

            Some(
                state @ (UsdcRebalance::BridgingSubmitting { .. }
                | UsdcRebalance::Bridging { .. }
                | UsdcRebalance::AwaitingAttestation { .. }
                | UsdcRebalance::Attested { .. }),
            ) => Err(UsdcTransferError::StateOffHop {
                id: id.clone(),
                state: state.state_name(),
                hop: HopKind::Relay,
            }),
        }
    }

    /// A fresh chain-to-Alpaca transfer. The pre-flight quote runs before
    /// anything is recorded, so a refusal moves nothing.
    async fn execute_chain_to_alpaca_via_relay(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
    ) -> Result<(), UsdcTransferError> {
        self.gas_readiness
            .ensure_ready(TransferGasRoute::Usdc)
            .await?;

        let amount_u256 = usdc_to_u256(amount)?;
        let preflight = self.accepted_quote(id, amount_u256).await?;
        info!(target: "rebalance", %id, %amount, request_id = %preflight.request_id, "Relay pre-flight quote accepted");

        self.withdraw_from_vault(id, amount, amount_u256).await?;
        self.quote_swap_after_withdrawal(id, amount).await
    }

    /// Records the binding quote on `WithdrawalComplete` and sends the
    /// deposit. A refused quote leaves the transfer at `WithdrawalComplete`
    /// with its guard held: the withdrawn stable is in the chain wallet.
    async fn quote_swap_after_withdrawal(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
    ) -> Result<(), UsdcTransferError> {
        let amount_u256 = usdc_to_u256(amount)?;
        let origin_from_block = self
            .hop
            .bridge
            .origin_block(TO_HUB)
            .await
            .map_err(Box::new)?;
        let quote = self
            .accepted_quote(id, amount_u256)
            .await
            .inspect_err(|error| {
                error!(
                    target: "rebalance",
                    %id,
                    %error,
                    "Binding Relay quote refused after the vault withdrawal; the transfer holds \
                     its guard at WithdrawalComplete"
                );
            })?;

        self.cqrs
            .send(
                id,
                UsdcRebalanceCommand::QuoteSwap {
                    quote: Box::new(swap_quote(
                        &quote,
                        self.hop.bounds.slippage_bps,
                        origin_from_block,
                    )),
                },
            )
            .await?;

        self.send_swap_deposit(id).await
    }

    /// A quote for `amount_in` of the chain's stable, accepted only within
    /// the corridor's bounds.
    async fn accepted_quote(
        &self,
        id: &UsdcRebalanceId,
        amount_in: U256,
    ) -> Result<RelayQuote, UsdcTransferError> {
        let RelayHopCtx {
            slippage_bps,
            max_quote_loss_bps,
            fill_timeout,
            ..
        } = self.hop.bounds;

        let request = QuoteRequest {
            origin: self.corridor.chain(),
            destination: Chain::Ethereum,
            amount: amount_in,
            user: self.market_maker_wallet,
            recipient: self.hop.hub_wallet,
            refund_to: self.market_maker_wallet,
            slippage: basis_points(slippage_bps)?,
            ttl: fill_timeout,
        };
        let quote = self.hop.client.quote(&request).await.map_err(Box::new)?;

        // The chain-to-Alpaca leg after the hop is the Alpaca deposit, which
        // sets no minimum; the trigger's `min_transfer` sizes against it.
        let bounds = QuoteBounds {
            max_loss: basis_points(max_quote_loss_bps)?,
            downstream_minimum: U256::ZERO,
        };
        quote.amounts.accept(&bounds).map_err(|source| {
            UsdcTransferError::SwapQuoteOutOfBounds {
                id: id.clone(),
                source,
            }
        })?;

        Ok(quote)
    }

    /// Signs and persists the pair, or picks up the one already persisted,
    /// then broadcasts it and records the confirmed deposit.
    async fn send_swap_deposit(&self, id: &UsdcRebalanceId) -> Result<(), UsdcTransferError> {
        let Some((pair, order_id)) = self.prepare_and_persist_swap_pair(id).await? else {
            return Ok(());
        };

        let deposit_tx = self
            .hop
            .bridge
            .broadcast_deposit(TO_HUB, &pair)
            .await
            .map_err(Box::new)?;
        let deposit = self
            .hop
            .bridge
            .confirm_deposit(TO_HUB, RelayOrderId(order_id), deposit_tx)
            .await
            .map_err(Box::new)?;

        self.cqrs
            .send(
                id,
                UsdcRebalanceCommand::ConfirmSwapDeposit {
                    deposit_tx,
                    deposit_block: deposit.block,
                },
            )
            .await?;

        info!(
            target: "rebalance",
            %id,
            %deposit_tx,
            block = deposit.block,
            "Relay deposit confirmed; the transfer waits at SwapDeposited for the fill"
        );
        Ok(())
    }

    /// The pair to broadcast and its order id, `None` once the deposit is
    /// confirmed. Runs on a detached task, as the Alpaca deposit send's
    /// prepare does, so a job timeout cannot drop the future between the
    /// signing and the persist: a pair signed and never persisted keeps its
    /// nonces reserved and stalls every later send from the chain wallet.
    async fn prepare_and_persist_swap_pair(
        &self,
        id: &UsdcRebalanceId,
    ) -> Result<Option<(PreparedSwapDeposit, B256)>, UsdcTransferError> {
        let hop = Arc::clone(&self.hop);
        let cqrs = Arc::clone(&self.cqrs);
        let task_id = id.clone();
        // `PrepareSwapDeposit` can outlive a cancelled job, so the task
        // continues the job's projection slot.
        let projection_slot =
            crate::conductor::projection_pause::projection_slot_for_detached_work().await;

        tokio::spawn(async move {
            let _projection_slot = projection_slot;
            hop.prepare_swap_pair(&cqrs, &task_id).await
        })
        .await
        .map_err(|join_error| prepare_panicked(id, &join_error))?
    }

    /// Reserves the nonces of every Relay envelope this corridor signed on
    /// its chain's wallet and persisted but did not confirm, and sends each
    /// again. A send whose receipt the bridge cannot read here counts as
    /// unmined, so startup skips that chain's approvals and revokes.
    pub(crate) async fn restore_chain_signed_swaps(
        &self,
        pool: &SqlitePool,
    ) -> BTreeMap<Chain, RestoredDepositSends> {
        let chain = self.corridor.chain();
        let mut outcome = RestoredDepositSends::default();

        let (ids, unparseable) = match prepared_swap_ids(pool).await {
            Ok(found) => found,
            Err(error) => {
                error!(target: "operational_alert", alert = true, %chain, ?error, "Could not list signed Relay pairs at startup; their nonces are not reserved until each transfer resumes");
                outcome.unmined += 1;
                return BTreeMap::from([(chain, outcome)]);
            }
        };
        if !unparseable.is_empty() {
            error!(target: "operational_alert", alert = true, %chain, ?unparseable, "Signed Relay pairs with unparseable transfer ids were not restored at startup");
            outcome.unmined += unparseable.len();
        }

        for id in ids {
            match self.cqrs.load(&id).await {
                Ok(Some(state))
                    if state.corridor() == self.corridor
                        && state.direction() == RebalanceDirection::BaseToAlpaca =>
                {
                    let restored = self.restore_swap_envelopes(&id, &state).await;
                    outcome.restored += restored;
                    outcome.unmined += restored;
                }
                Ok(Some(_)) => {}
                Ok(None) => {
                    warn!(target: "rebalance", %id, "Signed Relay pair has events but no state at startup");
                }
                Err(error) => {
                    error!(target: "operational_alert", alert = true, %id, ?error, "Could not load a transfer with a signed Relay pair at startup; its nonces are not reserved until it resumes");
                    outcome.unmined += 1;
                }
            }
        }

        BTreeMap::from([(chain, outcome)])
    }

    /// Restores and sends again the chain-signed envelopes of `state` in
    /// nonce order (lone approves, then the pair), and returns how many it
    /// restored.
    async fn restore_swap_envelopes(&self, id: &UsdcRebalanceId, state: &UsdcRebalance) -> usize {
        let (split_approves, pair) = match state {
            UsdcRebalance::SwapQuoted { split_approves, .. } => (split_approves.as_slice(), None),
            UsdcRebalance::SwapDepositPrepared {
                split_approves,
                approve,
                deposit,
                ..
            } => (
                split_approves.as_slice(),
                Some(PreparedSwapDeposit {
                    approve: approve.clone(),
                    deposit: deposit.clone(),
                }),
            ),
            UsdcRebalance::Converting { .. }
            | UsdcRebalance::ConversionComplete { .. }
            | UsdcRebalance::ConversionFailed { .. }
            | UsdcRebalance::WithdrawalSubmitting { .. }
            | UsdcRebalance::Withdrawing { .. }
            | UsdcRebalance::WithdrawalComplete { .. }
            | UsdcRebalance::WithdrawalFailed { .. }
            | UsdcRebalance::BridgingSubmitting { .. }
            | UsdcRebalance::Bridging { .. }
            | UsdcRebalance::AwaitingAttestation { .. }
            | UsdcRebalance::Attested { .. }
            | UsdcRebalance::SwapDeposited { .. }
            | UsdcRebalance::Bridged { .. }
            | UsdcRebalance::BridgingFailed { .. }
            | UsdcRebalance::DepositInitiated { .. }
            | UsdcRebalance::DepositConfirmed { .. }
            | UsdcRebalance::DepositFailed { .. }
            | UsdcRebalance::Reconciled { .. } => {
                warn!(target: "rebalance", %id, state = state.state_name(), "Transfer holds no signed Relay envelope at startup");
                return 0;
            }
        };

        for approve in split_approves {
            let lone = PreparedSwap::ApproveOnly {
                approve: approve.clone(),
            };
            self.hop.bridge.restore_prepared(TO_HUB, &lone).await;
        }
        if let Some(pair) = &pair {
            let signed = PreparedSwap::Deposit(pair.clone());
            self.hop.bridge.restore_prepared(TO_HUB, &signed).await;
        }

        for approve in split_approves {
            if let Err(error) = self.hop.bridge.broadcast_approve(TO_HUB, approve).await {
                error!(target: "operational_alert", alert = true, %id, tx = %approve.tx_hash(), nonce = approve.nonce(), ?error, "Could not rebroadcast a lone Relay approve at startup; its nonce stays reserved and the transfer's resume sends it again");
            }
        }
        if let Some(pair) = &pair
            && let Err(error) = self.hop.bridge.broadcast_deposit(TO_HUB, pair).await
        {
            error!(target: "operational_alert", alert = true, %id, deposit = %pair.deposit.tx_hash(), nonce = pair.deposit.nonce(), ?error, "Could not rebroadcast a signed Relay pair at startup; its nonces stay reserved and the transfer's resume sends it again");
        }

        let restored = split_approves.len()
            + pair
                .as_ref()
                .map_or(0, |pair| usize::from(pair.approve.is_some()) + 1);
        info!(target: "rebalance", %id, restored, "Reserved the nonces of the signed Relay envelopes");

        restored
    }
}

/// Pages that a Relay pair prepare panicked: nonces it reserved and did not
/// persist are never released, so later sends from the chain wallet wait
/// behind them until a restart.
fn prepare_panicked(id: &UsdcRebalanceId, join_error: &JoinError) -> UsdcTransferError {
    error!(target: "operational_alert", alert = true, %id, %join_error, "The Relay pair prepare task panicked; nonces it reserved and did not persist stall later sends from the chain wallet until a restart");
    UsdcTransferError::SwapPrepareTaskPanicked { id: id.clone() }
}

/// The least time a quote must have left before its deadline for its pair
/// to be signed: the approve and deposit still have to mine on the origin
/// chain and reach Relay's solver before the deadline.
const QUOTE_DEADLINE_HEADROOM: chrono::TimeDelta = chrono::TimeDelta::seconds(60);

/// Whether `quote`, recorded at `quoted_at`, is within
/// [`QUOTE_DEADLINE_HEADROOM`] of its deadline or older than `max_age` at
/// `now`. An age that does not fit counts as expired.
fn quote_expired(
    quote: &SwapQuote,
    quoted_at: DateTime<Utc>,
    max_age: Duration,
    now: DateTime<Utc>,
) -> bool {
    let stale_at = chrono::TimeDelta::from_std(max_age)
        .ok()
        .and_then(|max_age| quoted_at.checked_add_signed(max_age));
    let too_late = now.checked_add_signed(QUOTE_DEADLINE_HEADROOM);

    too_late.is_none_or(|too_late| too_late >= quote.deadline)
        || stale_at.is_none_or(|stale_at| now >= stale_at)
}

/// The persisted form of an accepted quote.
fn swap_quote(quote: &RelayQuote, slippage_bps: u16, origin_from_block: u64) -> SwapQuote {
    let RelayRequestId(request_id) = quote.request_id;
    let RelayOrderId(order_id) = quote.order_id;

    SwapQuote {
        request_id,
        order_id,
        amount_in: quote.amounts.amount_in,
        expected_out: quote.amounts.expected_out,
        minimum_out: quote.amounts.minimum_out,
        slippage_bps,
        relayer_fee: quote.fees.relayer,
        gas_fee: quote.fees.gas,
        deadline: DateTime::<Utc>::from(quote.deadline),
        approve: quote.approve.as_ref().map(swap_step),
        deposit: swap_step(&quote.deposit),
        origin_from_block,
    }
}

fn swap_step(step: &StepTransaction) -> SwapStep {
    SwapStep {
        chain_id: step.chain_id,
        to: step.to,
        data: step.data.clone(),
        value: step.value,
    }
}

fn step_transaction(step: &SwapStep) -> StepTransaction {
    StepTransaction {
        chain_id: step.chain_id,
        to: step.to,
        data: step.data.clone(),
        value: step.value,
    }
}

/// The quote a persisted `SwapQuote` was recorded from, without its approve
/// when `with_approve` is false.
fn relay_quote(quote: &SwapQuote, with_approve: bool) -> Result<RelayQuote, UsdcTransferError> {
    Ok(RelayQuote {
        request_id: RelayRequestId(quote.request_id),
        order_id: RelayOrderId(quote.order_id),
        amounts: QuoteAmounts {
            amount_in: quote.amount_in,
            expected_out: quote.expected_out,
            minimum_out: quote.minimum_out,
            slippage: basis_points(quote.slippage_bps)?,
        },
        deadline: SystemTime::from(quote.deadline),
        fees: QuoteFees {
            relayer: quote.relayer_fee,
            gas: quote.gas_fee,
        },
        approve: quote
            .approve
            .as_ref()
            .filter(|_| with_approve)
            .map(step_transaction),
        deposit: step_transaction(&quote.deposit),
    })
}

fn basis_points(bps: u16) -> Result<BasisPoints, UsdcTransferError> {
    BasisPoints::new(bps).map_err(UsdcTransferError::from)
}

/// Relay serves the hub legs through its bridge's Ethereum end.
#[async_trait]
impl<Signer: Wallet> UsdcBridgeHelper for RelayHop<Signer> {
    async fn ethereum_tx_confirmations(&self, tx_hash: TxHash) -> Result<Option<u64>, CctpError> {
        self.bridge.ethereum_tx_confirmations(tx_hash).await
    }

    async fn ethereum_tx_block(&self, tx_hash: TxHash) -> Result<u64, CctpError> {
        self.bridge.ethereum_tx_block(tx_hash).await
    }

    async fn ethereum_mined_tx(&self, tx_hash: TxHash) -> Result<Option<MinedTx>, CctpError> {
        self.bridge.ethereum_mined_tx(tx_hash).await
    }

    async fn ethereum_usdc_balance(&self, holder: Address) -> Result<U256, CctpError> {
        self.bridge.ethereum_usdc_balance(holder).await
    }

    async fn ethereum_usdc_credit(
        &self,
        tx_hash: TxHash,
        recipient: Address,
    ) -> Result<U256, CctpError> {
        self.bridge.ethereum_usdc_credit(tx_hash, recipient).await
    }

    async fn ethereum_usdc_sent(
        &self,
        tx_hash: TxHash,
        sender: Address,
        recipient: Address,
    ) -> Result<U256, CctpError> {
        self.bridge
            .ethereum_usdc_sent(tx_hash, sender, recipient)
            .await
    }

    async fn prepare_usdc_on_ethereum(
        &self,
        to: Address,
        amount: U256,
    ) -> Result<PreparedTransaction, CctpError> {
        self.bridge.prepare_usdc_on_ethereum(to, amount).await
    }

    async fn broadcast_usdc_on_ethereum(
        &self,
        prepared: &PreparedTransaction,
    ) -> Result<TxHash, CctpError> {
        self.bridge.broadcast_usdc_on_ethereum(prepared).await
    }

    async fn discard_usdc_on_ethereum(&self, prepared: &PreparedTransaction) {
        self.bridge.discard_usdc_on_ethereum(prepared).await;
    }

    async fn restore_usdc_on_ethereum(&self, prepared: &PreparedTransaction) {
        self.bridge.restore_usdc_on_ethereum(prepared).await;
    }

    async fn confirm_usdc_on_ethereum(
        &self,
        tx_hash: TxHash,
    ) -> Result<UsdcTransferStatus, CctpError> {
        self.bridge.confirm_usdc_on_ethereum(tx_hash).await
    }

    async fn find_recent_usdc_transfers(
        &self,
        from: Address,
        to: Address,
        amount: U256,
        from_block: u64,
    ) -> Result<Vec<TxHash>, CctpError> {
        self.bridge
            .find_recent_usdc_transfers(from, to, amount, from_block)
            .await
    }
}

#[async_trait]
impl<Signer> ResumeBaseToAlpaca for CrossVenueCashTransfer<Signer, RelayHop<Signer>>
where
    Signer: Wallet + Send + Sync + 'static,
{
    async fn resume_base_to_alpaca(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
        corridor: UsdcCorridor,
    ) -> Result<(), UsdcTransferError> {
        self.resume_chain_to_alpaca_via_relay(id, amount, corridor)
            .await
    }
}

/// The Alpaca-to-chain side of a Relay hop is not built yet: refused after
/// the corridor check and before any call.
#[async_trait]
impl<Signer> ResumeAlpacaToBase for CrossVenueCashTransfer<Signer, RelayHop<Signer>>
where
    Signer: Wallet + Send + Sync + 'static,
{
    async fn resume_alpaca_to_base(
        &self,
        id: &UsdcRebalanceId,
        _amount: Usdc,
        corridor: UsdcCorridor,
    ) -> Result<(), UsdcTransferError> {
        let state = self.cqrs.load(id).await?;
        self.require_served_corridor(id, corridor, state.as_ref())?;

        Err(UsdcTransferError::SwapDirectionNotBuilt {
            id: id.clone(),
            direction: RebalanceDirection::AlpacaToBase,
        })
    }
}

#[async_trait]
impl<Signer> RestorePreparedDepositSends for CrossVenueCashTransfer<Signer, RelayHop<Signer>>
where
    Signer: Wallet + Send + Sync + 'static,
{
    async fn restore_prepared_deposit_sends(&self, pool: &SqlitePool) -> RestoredDepositSends {
        Self::restore_prepared_deposit_sends(self, pool).await
    }

    async fn restore_chain_signed_swaps(
        &self,
        pool: &SqlitePool,
    ) -> BTreeMap<Chain, RestoredDepositSends> {
        Self::restore_chain_signed_swaps(self, pool).await
    }
}

#[async_trait]
impl<Signer> RecheckUsdcDeposit for CrossVenueCashTransfer<Signer, RelayHop<Signer>>
where
    Signer: Wallet + Send + Sync + 'static,
{
    /// No Relay transfer reaches a failed Alpaca deposit in this build, so
    /// every recorded state is refused before any call, the operator's
    /// deposit tx with it.
    async fn recheck_deposit(
        &self,
        id: &UsdcRebalanceId,
        _operator_deposit_tx: Option<TxHash>,
    ) -> Result<RecheckOutcome, UsdcRecheckError> {
        let state = self
            .cqrs
            .load(id)
            .await
            .map_err(|error| Box::new(UsdcTransferError::from(error)))?
            .ok_or_else(|| UsdcRecheckError::NotFound(id.clone()))?;
        self.require_served_corridor(id, self.corridor, Some(&state))
            .map_err(Box::new)?;

        Err(UsdcRecheckError::NotDepositFailed {
            id: id.clone(),
            state: state.state_name(),
        })
    }

    async fn verify_deposit_send_superseded(
        &self,
        id: &UsdcRebalanceId,
        prepared: &PreparedTransaction,
        superseding_tx: Option<TxHash>,
    ) -> Result<(), DepositSendNotSuperseded> {
        self.verify_hub_deposit_send_superseded(id, prepared, superseding_tx)
            .await
    }
}

/// A Relay corridor carries no CCTP burn to recover a mint for.
#[async_trait]
impl<Signer> RecoverCctpMint for CrossVenueCashTransfer<Signer, RelayHop<Signer>>
where
    Signer: Wallet + Send + Sync + 'static,
{
    async fn fetch_recovery_attestation(
        &self,
        _direction: BridgeDirection,
        _burn_tx: TxHash,
    ) -> Result<AttestationResponse, CctpMintRecoveryError> {
        Err(CctpMintRecoveryError::CorridorNotServed {
            corridor: UsdcCorridor::BASE_CCTP,
        })
    }

    async fn submit_recovered_cctp_mint(
        &self,
        _direction: BridgeDirection,
        _burn_tx: TxHash,
        _attestation: AttestationResponse,
    ) -> Result<RecoveredCctpMint, CctpMintRecoveryError> {
        Err(CctpMintRecoveryError::CorridorNotServed {
            corridor: UsdcCorridor::BASE_CCTP,
        })
    }
}

#[cfg(test)]
#[cfg(feature = "test-support")]
mod tests {
    use alloy::network::EthereumWallet;
    use alloy::node_bindings::Anvil;
    use alloy::primitives::{B256, Bytes, Signature};
    use alloy::providers::{Provider as _, ProviderBuilder, RootProvider};
    use alloy::rpc::types::TransactionReceipt;
    use alloy::signers::local::PrivateKeySigner;
    use alloy::sol;
    use alloy::sol_types::SolCall;
    use httpmock::prelude::*;
    use serde_json::json;
    use std::sync::Arc;
    use std::time::Duration;
    use uuid::Uuid;

    use st0x_bridge::relay::{QuoteErrorCode, RelayCtx, RelayEndContracts, RelayError};
    use st0x_event_sorcery::{Store, test_store};
    use st0x_evm::{Evm, EvmError, IERC20};
    use st0x_execution::{
        AlpacaAccountId, AlpacaBrokerApi, AlpacaBrokerApiCtx, AlpacaBrokerApiMode,
        AlpacaBrokerAuth, AlpacaWalletService, Executor as _, TimeInForce,
    };
    use st0x_float_macro::float;
    use st0x_raindex::{RaindexContracts, RaindexService, RaindexVaultId};

    use super::*;
    use crate::bot_gas::BotGasReceiptCostEnqueuer;
    use crate::rebalancing::usdc::{MarketMakingUsdcEndpoints, UsdcSettlementParams};
    use crate::telemetry::TelemetrySender;
    use crate::telemetry::broker::InstrumentedAlpacaBroker;
    use crate::test_utils::{TestAnvilInstance, anvil_wallet, setup_test_db, spawn_anvil};
    use crate::usdc_rebalance::TransferRef;

    type TestWallet = Arc<dyn Wallet<Provider = RootProvider>>;

    const ROBINHOOD_RELAY: UsdcCorridor = UsdcCorridor::HubRouted {
        chain: Chain::Robinhood,
        hop: HopKind::Relay,
    };

    sol! {
        #[sol(rpc)]
        interface MintableStable {
            function mint(address to, uint256 amount) external;
        }

        function depositErc20(address depositor, address token, uint256 amount, bytes32 id);
    }

    fn relay_bounds() -> RelayHopCtx {
        RelayHopCtx {
            slippage_bps: 30,
            max_quote_loss_bps: 50,
            min_transfer: Usdc::new(float!(500)),
            max_transfer: Usdc::new(float!(50000)),
            quote_max_age: Duration::from_secs(60),
            fill_timeout: Duration::from_secs(1800),
            max_refund_retries: 3,
            max_deposit_revert_redrives: 5,
        }
    }

    async fn alpaca_services(
        server: &MockServer,
    ) -> (InstrumentedAlpacaBroker, AlpacaWalletService) {
        let account_id = AlpacaAccountId::new(Uuid::nil());
        server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/trading/accounts/{account_id}/account"));
            then.status(200)
                .json_body(json!({"id": account_id.to_string(), "status": "ACTIVE"}));
        });
        let auth = AlpacaBrokerAuth::Basic {
            api_key: "test_key".to_string(),
            api_secret: "test_secret".to_string(),
        };
        let broker = AlpacaBrokerApi::try_from_ctx(AlpacaBrokerApiCtx {
            auth: auth.clone(),
            account_id,
            mode: Some(AlpacaBrokerApiMode::Mock(server.base_url())),
            asset_cache_ttl: Duration::from_secs(3600),
            time_in_force: TimeInForce::default(),
            counter_trade_slippage_bps: st0x_execution::DEFAULT_ALPACA_COUNTER_TRADE_SLIPPAGE_BPS,
            hedge_floor: st0x_execution::HedgeFloor::default(),
        })
        .await
        .unwrap();

        (
            InstrumentedAlpacaBroker::new(broker, TelemetrySender::disabled()),
            AlpacaWalletService::new(server.base_url(), account_id, auth).unwrap(),
        )
    }

    /// A Robinhood Relay service whose quotes come from `relay_api` and whose
    /// stable and depository are `contracts` on both ends.
    async fn relay_transfer(
        server: &MockServer,
        relay_api: &MockServer,
        ethereum_wallet: TestWallet,
        chain_wallet: TestWallet,
        contracts: RelayEndContracts,
        store: Arc<Store<UsdcRebalance>>,
    ) -> CrossVenueCashTransfer<TestWallet, RelayHop<TestWallet>> {
        relay_transfer_with_bounds(
            server,
            relay_api,
            ethereum_wallet,
            chain_wallet,
            contracts,
            store,
            relay_bounds(),
        )
        .await
    }

    async fn relay_transfer_with_bounds(
        server: &MockServer,
        relay_api: &MockServer,
        ethereum_wallet: TestWallet,
        chain_wallet: TestWallet,
        contracts: RelayEndContracts,
        store: Arc<Store<UsdcRebalance>>,
        bounds: RelayHopCtx,
    ) -> CrossVenueCashTransfer<TestWallet, RelayHop<TestWallet>> {
        let hub_wallet = ethereum_wallet.address();
        let bridge = RelayBridge::try_from_ctx(RelayCtx {
            chain: Chain::Robinhood,
            ethereum_wallet,
            chain_wallet: chain_wallet.clone(),
            ethereum_confirmations: 1,
            chain_confirmations: 1,
        })
        .unwrap()
        .with_local_contracts(contracts, contracts);
        let client = RelayClient::new(None)
            .unwrap()
            .with_api_base(relay_api.base_url());
        let (broker, alpaca_wallet) = alpaca_services(server).await;
        let raindex = Arc::new(RaindexService::new(
            chain_wallet.clone(),
            RaindexContracts {
                inventory: Address::repeat_byte(0x0b),
                orderbook: Address::repeat_byte(0x0b),
            },
            chain_wallet.address(),
        ));

        CrossVenueCashTransfer::new(
            broker,
            Arc::new(alpaca_wallet),
            Arc::new(RelayHop::new(bridge, client, bounds, hub_wallet)),
            raindex,
            store,
            MarketMakingUsdcEndpoints::new(
                ROBINHOOD_RELAY,
                chain_wallet.address(),
                RaindexVaultId(B256::repeat_byte(0x0c)),
            ),
            &UsdcSettlementParams {
                attestation_retry_deadline: Duration::from_secs(3600),
                settlement_retry_deadline: Duration::from_secs(3600),
                ethereum_required_confirmations: Some(1),
                reserved_cash: None,
                circle_api_base: st0x_bridge::cctp::CIRCLE_API_BASE.to_string(),
                token_messenger: st0x_bridge::cctp::TOKEN_MESSENGER_V2,
                message_transmitter: st0x_bridge::cctp::MESSAGE_TRANSMITTER_V2,
            },
            BotGasReceiptCostEnqueuer::Disabled,
        )
    }

    /// A refused pre-flight quote ends the attempt before anything is
    /// recorded: no withdrawal, no event, so the guard frees as for any
    /// transfer that recorded nothing.
    #[tokio::test]
    async fn quote_refused_before_withdrawal_moves_nothing() {
        let anvil = spawn_anvil(Anvil::new());
        let key = B256::from_slice(&anvil.keys()[0].to_bytes());
        let wallet = anvil_wallet(anvil.endpoint_url(), &key);
        let server = MockServer::start();
        let relay_api = MockServer::start();
        let quote = relay_api.mock(|when, then| {
            when.method(POST).path("/quote/v2");
            then.status(400).json_body(json!({
                "message": "Amount is too low",
                "errorCode": "AMOUNT_TOO_LOW",
                "requestId": "0x00"
            }));
        });
        let store = Arc::new(test_store(setup_test_db().await, ()));
        let contracts = RelayEndContracts {
            stable: Address::repeat_byte(0x51),
            depository: Address::repeat_byte(0x52),
        };
        let transfer = relay_transfer(
            &server,
            &relay_api,
            wallet.clone(),
            wallet,
            contracts,
            store.clone(),
        )
        .await;
        let id = UsdcRebalanceId(Uuid::new_v4());

        let error = transfer
            .resume_base_to_alpaca(&id, Usdc::new(float!(1000)), ROBINHOOD_RELAY)
            .await
            .unwrap_err();

        assert!(
            matches!(
                &error,
                UsdcTransferError::RelayApi(relay)
                    if matches!(**relay, RelayError::QuoteRefused { code: QuoteErrorCode::AmountTooLow })
            ),
            "got {error:?}"
        );
        quote.assert_calls(1);
        assert_eq!(store.load(&id).await.unwrap(), None);
    }

    /// A binding quote refused after the vault withdrawal leaves the
    /// transfer at `WithdrawalComplete`, its guard held: the withdrawn stable
    /// sits in the chain wallet and only a redeposit may return it.
    #[tokio::test]
    async fn binding_quote_refused_after_withdrawal_parks_at_withdrawal_complete() {
        let anvil = spawn_anvil(Anvil::new());
        let key = B256::from_slice(&anvil.keys()[0].to_bytes());
        let wallet = anvil_wallet(anvil.endpoint_url(), &key);
        let server = MockServer::start();
        let relay_api = MockServer::start();
        relay_api.mock(|when, then| {
            when.method(POST).path("/quote/v2");
            then.status(400).json_body(json!({
                "message": "No routes",
                "errorCode": "NO_SWAP_ROUTES_FOUND",
                "requestId": "0x00"
            }));
        });
        let store = Arc::new(test_store(setup_test_db().await, ()));
        let contracts = RelayEndContracts {
            stable: Address::repeat_byte(0x51),
            depository: Address::repeat_byte(0x52),
        };
        let transfer = relay_transfer(
            &server,
            &relay_api,
            wallet.clone(),
            wallet,
            contracts,
            store.clone(),
        )
        .await;
        let id = UsdcRebalanceId(Uuid::new_v4());
        for command in [
            UsdcRebalanceCommand::Initiate {
                direction: RebalanceDirection::BaseToAlpaca,
                corridor: ROBINHOOD_RELAY,
                amount: Usdc::new(float!(100)),
                withdrawal: TransferRef::OnchainTx(TxHash::repeat_byte(0x77)),
            },
            UsdcRebalanceCommand::ConfirmWithdrawal {
                withdrawal_tx: None,
            },
        ] {
            store.send(&id, command).await.unwrap();
        }

        let error = transfer
            .resume_base_to_alpaca(&id, Usdc::new(float!(100)), ROBINHOOD_RELAY)
            .await
            .unwrap_err();

        assert!(
            matches!(&error, UsdcTransferError::RelayApi(relay)
                if matches!(**relay, RelayError::QuoteRefused { code: QuoteErrorCode::NoSwapRoutesFound })),
            "got {error:?}"
        );
        let state = store.load(&id).await.unwrap().unwrap();
        assert_eq!(state.state_name(), "WithdrawalComplete");
        assert!(state.holds_rebalance_guard());
    }

    /// The Alpaca-to-chain side is refused before any call, and a CCTP mint
    /// recovery is never this service's.
    #[tokio::test]
    async fn relay_service_refuses_what_it_does_not_run() {
        let anvil = spawn_anvil(Anvil::new());
        let key = B256::from_slice(&anvil.keys()[0].to_bytes());
        let wallet = anvil_wallet(anvil.endpoint_url(), &key);
        let server = MockServer::start();
        let relay_api = MockServer::start();
        let store = Arc::new(test_store(setup_test_db().await, ()));
        let contracts = RelayEndContracts {
            stable: Address::repeat_byte(0x51),
            depository: Address::repeat_byte(0x52),
        };
        let transfer = relay_transfer(
            &server,
            &relay_api,
            wallet.clone(),
            wallet,
            contracts,
            store.clone(),
        )
        .await;
        let id = UsdcRebalanceId(Uuid::new_v4());

        let error = transfer
            .resume_alpaca_to_base(&id, Usdc::new(float!(100)), ROBINHOOD_RELAY)
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                UsdcTransferError::SwapDirectionNotBuilt {
                    direction: RebalanceDirection::AlpacaToBase,
                    ..
                }
            ),
            "got {error:?}"
        );
        assert_eq!(store.load(&id).await.unwrap(), None);

        let error = transfer
            .fetch_recovery_attestation(BridgeDirection::BaseToEthereum, TxHash::ZERO)
            .await
            .unwrap_err();
        assert!(
            matches!(error, CctpMintRecoveryError::CorridorNotServed { .. }),
            "got {error:?}"
        );
    }

    /// Delegates to the wrapped wallet, recording at each broadcast which
    /// envelopes the transfer had persisted at that moment.
    struct BroadcastWitness {
        inner: TestWallet,
        store: Arc<Store<UsdcRebalance>>,
        id: UsdcRebalanceId,
        seen: std::sync::Mutex<Vec<(TxHash, Vec<TxHash>)>>,
        on_deposit_signing: std::sync::Mutex<Option<OnDepositSigning>>,
    }

    /// What the wallet does, once, when the transfer signs a Relay deposit.
    enum OnDepositSigning {
        /// The signing returns this long after the deposit's nonce is taken.
        SlowAfterSigning(Duration),
        /// Another send from this wallet takes the next nonce first, and is
        /// broadcast so no nonce is left as a gap.
        AnotherSendFirst { to: Address },
        /// The signing panics before the deposit's nonce is taken.
        Panic,
    }

    impl BroadcastWitness {
        fn new(inner: TestWallet, store: Arc<Store<UsdcRebalance>>, id: UsdcRebalanceId) -> Self {
            Self {
                inner,
                store,
                id,
                seen: std::sync::Mutex::new(Vec::new()),
                on_deposit_signing: std::sync::Mutex::new(None),
            }
        }

        fn on_deposit_signing(self, hook: OnDepositSigning) -> Self {
            *self.on_deposit_signing.lock().unwrap() = Some(hook);
            self
        }

        fn broadcasts(&self) -> Vec<TxHash> {
            self.seen
                .lock()
                .unwrap()
                .iter()
                .map(|(tx, _)| *tx)
                .collect()
        }
    }

    #[async_trait]
    impl Evm for BroadcastWitness {
        type Provider = RootProvider;

        fn provider(&self) -> &RootProvider {
            self.inner.provider()
        }
    }

    #[async_trait]
    impl Wallet for BroadcastWitness {
        fn address(&self) -> Address {
            self.inner.address()
        }

        async fn sign_typed_data(
            &self,
            payload_json: String,
            expected_digest: B256,
        ) -> Result<Signature, EvmError> {
            self.inner
                .sign_typed_data(payload_json, expected_digest)
                .await
        }

        async fn prepare_pending(
            &self,
            contract: Address,
            calldata: Bytes,
            note: &str,
        ) -> Result<PreparedTransaction, EvmError> {
            self.inner.prepare_pending(contract, calldata, note).await
        }

        async fn prepare_pending_with_gas_limit(
            &self,
            contract: Address,
            calldata: Bytes,
            unpadded_gas_limit: u64,
            note: &str,
        ) -> Result<PreparedTransaction, EvmError> {
            let hook = self.on_deposit_signing.lock().unwrap().take();
            assert!(
                !matches!(hook, Some(OnDepositSigning::Panic)),
                "the deposit signing panics for the test"
            );
            if let Some(OnDepositSigning::AnotherSendFirst { to }) = hook {
                let calldata = IERC20::approveCall {
                    spender: Address::repeat_byte(0x99),
                    amount: U256::ZERO,
                }
                .abi_encode();
                let another = self
                    .inner
                    .prepare_pending(to, calldata.into(), "another send")
                    .await?;
                self.inner
                    .broadcast_prepared(&another, "another send")
                    .await?;
            }

            let prepared = self
                .inner
                .prepare_pending_with_gas_limit(contract, calldata, unpadded_gas_limit, note)
                .await?;

            if let Some(OnDepositSigning::SlowAfterSigning(delay)) = hook {
                tokio::time::sleep(delay).await;
            }

            Ok(prepared)
        }

        async fn broadcast_prepared(
            &self,
            prepared: &PreparedTransaction,
            note: &str,
        ) -> Result<TxHash, EvmError> {
            let persisted = self
                .store
                .load(&self.id)
                .await
                .unwrap()
                .map(|state| {
                    state
                        .prepared_swap_envelopes()
                        .into_iter()
                        .map(PreparedTransaction::tx_hash)
                        .collect()
                })
                .unwrap_or_default();
            self.seen
                .lock()
                .unwrap()
                .push((prepared.tx_hash(), persisted));

            self.inner.broadcast_prepared(prepared, note).await
        }

        async fn discard_prepared(&self, tx_hash: TxHash) {
            self.inner.discard_prepared(tx_hash).await;
        }

        async fn prepare_fee_replacement(
            &self,
            prepared: &PreparedTransaction,
        ) -> Result<Option<PreparedTransaction>, EvmError> {
            self.inner.prepare_fee_replacement(prepared).await
        }

        async fn release_superseded(&self, tx_hash: TxHash) {
            self.inner.release_superseded(tx_hash).await;
        }

        async fn restore_prepared(&self, prepared: &PreparedTransaction) {
            self.inner.restore_prepared(prepared).await;
        }

        async fn restore_transaction(&self, tx_hash: TxHash) -> Result<(), EvmError> {
            self.inner.restore_transaction(tx_hash).await
        }

        async fn send_pending(
            &self,
            contract: Address,
            calldata: Bytes,
            note: &str,
        ) -> Result<TxHash, EvmError> {
            self.inner.send_pending(contract, calldata, note).await
        }

        async fn await_receipt(&self, tx_hash: TxHash) -> Result<TransactionReceipt, EvmError> {
            self.inner.await_receipt(tx_hash).await
        }

        async fn send(
            &self,
            contract: Address,
            calldata: Bytes,
            note: &str,
        ) -> Result<TransactionReceipt, EvmError> {
            self.inner.send(contract, calldata, note).await
        }
    }

    /// The quote's input, in the stable's base units.
    const AMOUNT_IN: u64 = 100_000_000;

    /// A deployed Relay end on `anvil` whose stable credits the bot wallet
    /// (anvil's first key) with `AMOUNT_IN`.
    async fn funded_relay_end(anvil: &TestAnvilInstance) -> (TestWallet, RelayEndContracts) {
        let key = B256::from_slice(&anvil.keys()[0].to_bytes());
        let wallet = anvil_wallet(anvil.endpoint_url(), &key);
        let deployer = ProviderBuilder::new()
            .wallet(EthereumWallet::from(
                PrivateKeySigner::from_slice(&anvil.keys()[1].to_bytes()).unwrap(),
            ))
            .connect_http(anvil.endpoint_url());
        let contracts = st0x_bridge::relay::deploy_relay_end(&deployer)
            .await
            .unwrap();
        MintableStable::new(contracts.stable, &deployer)
            .mint(wallet.address(), U256::from(AMOUNT_IN))
            .send()
            .await
            .unwrap()
            .watch()
            .await
            .unwrap();

        (wallet, contracts)
    }

    /// A live quote for `AMOUNT_IN` whose approve and deposit call
    /// `contracts` for `wallet`.
    async fn exact_quote(
        wallet: &TestWallet,
        contracts: RelayEndContracts,
        order_id: B256,
    ) -> SwapQuote {
        let amount_in = U256::from(AMOUNT_IN);
        let chain_id = wallet.provider().get_chain_id().await.unwrap();
        let mut quote = crate::usdc_rebalance::swap_quote_for_test(amount_in, order_id);
        quote.deadline = Utc::now() + chrono::Duration::hours(1);
        quote.approve = Some(SwapStep {
            chain_id,
            to: contracts.stable,
            data: IERC20::approveCall {
                spender: contracts.depository,
                amount: amount_in,
            }
            .abi_encode()
            .into(),
            value: U256::ZERO,
        });
        quote.deposit = SwapStep {
            chain_id,
            to: contracts.depository,
            data: depositErc20Call {
                depositor: wallet.address(),
                token: contracts.stable,
                amount: amount_in,
                id: order_id,
            }
            .abi_encode()
            .into(),
            value: U256::ZERO,
        };

        quote
    }

    /// Records a Robinhood Relay transfer of `AMOUNT_IN` up to `SwapQuoted`.
    async fn record_quoted(store: &Store<UsdcRebalance>, id: &UsdcRebalanceId, quote: SwapQuote) {
        for command in [
            UsdcRebalanceCommand::Initiate {
                direction: RebalanceDirection::BaseToAlpaca,
                corridor: ROBINHOOD_RELAY,
                amount: Usdc::new(float!(100)),
                withdrawal: TransferRef::OnchainTx(TxHash::repeat_byte(0x77)),
            },
            UsdcRebalanceCommand::ConfirmWithdrawal {
                withdrawal_tx: None,
            },
            UsdcRebalanceCommand::QuoteSwap {
                quote: Box::new(quote),
            },
        ] {
            store.send(id, command).await.unwrap();
        }
    }

    /// The approve and the deposit are persisted in one event before either
    /// is broadcast, and the deposit recorded is the persisted one.
    #[tokio::test]
    async fn relay_step_persists_both_envelopes_before_broadcast() {
        let anvil = spawn_anvil(Anvil::new());
        let (wallet, contracts) = funded_relay_end(&anvil).await;
        let store = Arc::new(test_store(setup_test_db().await, ()));
        let id = UsdcRebalanceId(Uuid::new_v4());
        let witness = Arc::new(BroadcastWitness::new(
            wallet.clone(),
            store.clone(),
            id.clone(),
        ));
        let chain_wallet: TestWallet = witness.clone();
        let server = MockServer::start();
        let relay_api = MockServer::start();
        let transfer = relay_transfer(
            &server,
            &relay_api,
            wallet.clone(),
            chain_wallet,
            contracts,
            store.clone(),
        )
        .await;

        let order_id = B256::repeat_byte(0x0d);
        record_quoted(&store, &id, exact_quote(&wallet, contracts, order_id).await).await;

        transfer
            .resume_base_to_alpaca(&id, Usdc::new(float!(100)), ROBINHOOD_RELAY)
            .await
            .unwrap();

        let state = store.load(&id).await.unwrap().unwrap();
        let UsdcRebalance::SwapDeposited {
            deposit_tx,
            signed_order_ids,
            ..
        } = &state
        else {
            panic!("expected SwapDeposited, got {state:?}");
        };
        assert_eq!(*signed_order_ids, vec![order_id]);

        let seen = witness.seen.lock().unwrap().clone();
        let broadcast = witness.broadcasts();
        assert_eq!(
            broadcast.len(),
            2,
            "the approve, then the deposit: {seen:?}"
        );
        assert_eq!(broadcast[1], *deposit_tx);
        for (tx, persisted) in &seen {
            assert_eq!(
                *persisted, broadcast,
                "both envelopes must be persisted before {tx} is broadcast"
            );
        }
    }

    /// A job timeout that cancels the attempt after the pair is signed and
    /// before it is persisted must not split the two: the pair is persisted
    /// at the nonces it took, and the redrive sends it.
    #[tokio::test]
    async fn cancelled_attempt_still_persists_its_signed_pair() {
        let anvil = spawn_anvil(Anvil::new());
        let (wallet, contracts) = funded_relay_end(&anvil).await;
        let store = Arc::new(test_store(setup_test_db().await, ()));
        let id = UsdcRebalanceId(Uuid::new_v4());
        let witness = Arc::new(
            BroadcastWitness::new(wallet.clone(), store.clone(), id.clone())
                .on_deposit_signing(OnDepositSigning::SlowAfterSigning(Duration::from_secs(1))),
        );
        let chain_wallet: TestWallet = witness.clone();
        let server = MockServer::start();
        let relay_api = MockServer::start();
        let transfer = relay_transfer(
            &server,
            &relay_api,
            wallet.clone(),
            chain_wallet,
            contracts,
            store.clone(),
        )
        .await;
        record_quoted(
            &store,
            &id,
            exact_quote(&wallet, contracts, B256::repeat_byte(0x0d)).await,
        )
        .await;
        let first_nonce = wallet
            .provider()
            .get_transaction_count(wallet.address())
            .await
            .unwrap();

        let Err(_elapsed) = tokio::time::timeout(
            Duration::from_millis(300),
            transfer.resume_base_to_alpaca(&id, Usdc::new(float!(100)), ROBINHOOD_RELAY),
        )
        .await
        else {
            panic!("the attempt must time out while the deposit is being signed");
        };

        let persisted = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(UsdcRebalance::SwapDepositPrepared {
                    approve, deposit, ..
                }) = store.load(&id).await.unwrap()
                {
                    return (approve, deposit);
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        let Ok((approve, deposit)) = persisted else {
            panic!(
                "the cancelled attempt's signed pair must still be persisted, got {:?}",
                store.load(&id).await.unwrap()
            );
        };
        assert_eq!(
            approve.as_ref().map(PreparedTransaction::nonce),
            Some(first_nonce)
        );
        assert_eq!(deposit.nonce(), first_nonce + 1);

        tokio::time::timeout(
            Duration::from_secs(30),
            transfer.resume_base_to_alpaca(&id, Usdc::new(float!(100)), ROBINHOOD_RELAY),
        )
        .await
        .expect("the redrive sends the persisted pair")
        .unwrap();

        let state = store.load(&id).await.unwrap().unwrap();
        let UsdcRebalance::SwapDeposited { deposit_tx, .. } = state else {
            panic!("expected SwapDeposited, got {state:?}");
        };
        assert_eq!(deposit_tx, deposit.tx_hash());
    }

    /// A quote past its deadline, too close to it to mine in time, or older
    /// than the corridor's `quote_max_age`, is never signed: the transfer
    /// stays at `SwapQuoted` with its guard held and nothing is broadcast.
    #[tokio::test]
    async fn expired_swap_quote_is_not_signed() {
        let past_deadline = (-chrono::Duration::minutes(1), Duration::from_secs(60));
        let near_deadline = (chrono::Duration::seconds(30), Duration::from_secs(60));
        let too_old = (chrono::Duration::hours(1), Duration::from_millis(50));

        for (deadline_in, quote_max_age) in [past_deadline, near_deadline, too_old] {
            let anvil = spawn_anvil(Anvil::new());
            let (wallet, contracts) = funded_relay_end(&anvil).await;
            let store = Arc::new(test_store(setup_test_db().await, ()));
            let id = UsdcRebalanceId(Uuid::new_v4());
            let witness = Arc::new(BroadcastWitness::new(
                wallet.clone(),
                store.clone(),
                id.clone(),
            ));
            let chain_wallet: TestWallet = witness.clone();
            let server = MockServer::start();
            let relay_api = MockServer::start();
            let transfer = relay_transfer_with_bounds(
                &server,
                &relay_api,
                wallet.clone(),
                chain_wallet,
                contracts,
                store.clone(),
                RelayHopCtx {
                    quote_max_age,
                    ..relay_bounds()
                },
            )
            .await;
            let mut quote = exact_quote(&wallet, contracts, B256::repeat_byte(0x0d)).await;
            quote.deadline = Utc::now() + deadline_in;
            record_quoted(&store, &id, quote).await;
            tokio::time::sleep(Duration::from_millis(100)).await;

            let error = tokio::time::timeout(
                Duration::from_secs(30),
                transfer.resume_base_to_alpaca(&id, Usdc::new(float!(100)), ROBINHOOD_RELAY),
            )
            .await
            .expect("the attempt ends")
            .unwrap_err();
            assert!(
                matches!(&error, UsdcTransferError::SwapQuoteExpired { id: refused, .. } if *refused == id),
                "got {error:?}"
            );

            let state = store.load(&id).await.unwrap().unwrap();
            assert_eq!(state.state_name(), "SwapQuoted", "{state:?}");
            assert!(state.holds_rebalance_guard());
            assert_eq!(witness.broadcasts(), Vec::<TxHash>::new());
        }
    }

    /// A panic while the pair is signed pages an operational alert: the
    /// nonces it reserved stall the chain wallet until a restart.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn panicked_swap_prepare_pages_an_operational_alert() {
        let anvil = spawn_anvil(Anvil::new());
        let (wallet, contracts) = funded_relay_end(&anvil).await;
        let store = Arc::new(test_store(setup_test_db().await, ()));
        let id = UsdcRebalanceId(Uuid::new_v4());
        let witness = Arc::new(
            BroadcastWitness::new(wallet.clone(), store.clone(), id.clone())
                .on_deposit_signing(OnDepositSigning::Panic),
        );
        let chain_wallet: TestWallet = witness.clone();
        let server = MockServer::start();
        let relay_api = MockServer::start();
        let transfer = relay_transfer(
            &server,
            &relay_api,
            wallet.clone(),
            chain_wallet,
            contracts,
            store.clone(),
        )
        .await;
        record_quoted(
            &store,
            &id,
            exact_quote(&wallet, contracts, B256::repeat_byte(0x0d)).await,
        )
        .await;

        let error = transfer
            .resume_base_to_alpaca(&id, Usdc::new(float!(100)), ROBINHOOD_RELAY)
            .await
            .unwrap_err();

        assert!(
            matches!(&error, UsdcTransferError::SwapPrepareTaskPanicked { id: panicked } if *panicked == id),
            "got {error:?}"
        );
        logs_assert(|lines: &[&str]| {
            lines
                .iter()
                .any(|line| {
                    line.contains("operational_alert")
                        && line.contains("stall later sends from the chain wallet until a restart")
                })
                .then_some(())
                .ok_or_else(|| "no operational alert says the nonces stall the wallet".to_string())
        });
        assert_eq!(
            store.load(&id).await.unwrap().unwrap().state_name(),
            "SwapQuoted"
        );
    }

    /// A signing that outlasts the corridor's `quote_max_age` ends the
    /// attempt instead of holding the chain wallet's prepare lock, and the
    /// pair it signs late is discarded, so its nonces go to the next send.
    #[tokio::test]
    async fn hung_swap_signing_times_out_and_releases_its_nonces() {
        let anvil = spawn_anvil(Anvil::new());
        let (wallet, contracts) = funded_relay_end(&anvil).await;
        let store = Arc::new(test_store(setup_test_db().await, ()));
        let id = UsdcRebalanceId(Uuid::new_v4());
        let witness = Arc::new(
            BroadcastWitness::new(wallet.clone(), store.clone(), id.clone())
                .on_deposit_signing(OnDepositSigning::SlowAfterSigning(Duration::from_secs(3))),
        );
        let chain_wallet: TestWallet = witness.clone();
        let server = MockServer::start();
        let relay_api = MockServer::start();
        let transfer = relay_transfer_with_bounds(
            &server,
            &relay_api,
            wallet.clone(),
            chain_wallet,
            contracts,
            store.clone(),
            RelayHopCtx {
                quote_max_age: Duration::from_secs(1),
                ..relay_bounds()
            },
        )
        .await;
        let quote = exact_quote(&wallet, contracts, B256::repeat_byte(0x0d)).await;
        let approve_step = quote.approve.clone().unwrap();
        record_quoted(&store, &id, quote).await;
        let first_nonce = wallet
            .provider()
            .get_transaction_count(wallet.address())
            .await
            .unwrap();

        let started = std::time::Instant::now();
        let error = tokio::time::timeout(
            Duration::from_secs(10),
            transfer.resume_base_to_alpaca(&id, Usdc::new(float!(100)), ROBINHOOD_RELAY),
        )
        .await
        .expect("the attempt ends")
        .unwrap_err();
        assert!(
            matches!(&error, UsdcTransferError::SwapPrepareTimedOut { id: timed_out, .. } if *timed_out == id),
            "got {error:?}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "the attempt must end at the bound, not when the signing returns"
        );
        assert_eq!(
            store.load(&id).await.unwrap().unwrap().state_name(),
            "SwapQuoted"
        );
        assert_eq!(witness.broadcasts(), Vec::<TxHash>::new());

        tokio::time::sleep(Duration::from_secs(4)).await;
        let next = wallet
            .prepare_pending(contracts.stable, approve_step.data, "next send")
            .await
            .unwrap();
        assert_eq!(
            next.nonce(),
            first_nonce,
            "the late pair's nonces are released"
        );
    }

    /// A pair persisted after an approve went out alone is resumed in
    /// process: the lone approve is broadcast again before the pair, so the
    /// deposit is not left behind a nonce the node may not hold.
    #[tokio::test]
    async fn resumed_prepared_pair_rebroadcasts_its_split_approves_first() {
        let anvil = spawn_anvil(Anvil::new());
        let (wallet, contracts) = funded_relay_end(&anvil).await;
        let store = Arc::new(test_store(setup_test_db().await, ()));
        let id = UsdcRebalanceId(Uuid::new_v4());
        let witness = Arc::new(BroadcastWitness::new(
            wallet.clone(),
            store.clone(),
            id.clone(),
        ));
        let chain_wallet: TestWallet = witness.clone();
        let server = MockServer::start();
        let relay_api = MockServer::start();
        let transfer = relay_transfer(
            &server,
            &relay_api,
            wallet.clone(),
            chain_wallet,
            contracts,
            store.clone(),
        )
        .await;
        let quote = exact_quote(&wallet, contracts, B256::repeat_byte(0x0d)).await;
        let approve_step = quote.approve.clone().unwrap();
        let deposit_step = quote.deposit.clone();
        record_quoted(&store, &id, quote).await;

        let approve = wallet
            .prepare_pending(contracts.stable, approve_step.data, "split approve")
            .await
            .unwrap();
        store
            .send(
                &id,
                UsdcRebalanceCommand::PrepareSwapApprove {
                    approve: approve.clone(),
                },
            )
            .await
            .unwrap();
        let deposit = wallet
            .prepare_pending_with_gas_limit(
                contracts.depository,
                deposit_step.data,
                200_000,
                "deposit",
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                UsdcRebalanceCommand::PrepareSwapDeposit {
                    approve: None,
                    deposit: deposit.clone(),
                },
            )
            .await
            .unwrap();

        let resumed = tokio::time::timeout(
            Duration::from_secs(10),
            transfer.resume_base_to_alpaca(&id, Usdc::new(float!(100)), ROBINHOOD_RELAY),
        )
        .await;

        assert_eq!(
            witness.broadcasts().first(),
            Some(&approve.tx_hash()),
            "the lone approve must be broadcast before the deposit"
        );
        resumed
            .expect("the deposit mines once its approve is sent")
            .unwrap();
        let state = store.load(&id).await.unwrap().unwrap();
        let UsdcRebalance::SwapDeposited { deposit_tx, .. } = state else {
            panic!("expected SwapDeposited, got {state:?}");
        };
        assert_eq!(deposit_tx, deposit.tx_hash());
    }

    /// Another send takes the nonce between the approve and the deposit: the
    /// approve is persisted on `SwapQuoted` and goes out alone, and the next
    /// attempt sends it again, signs the deposit alone at the nonce after
    /// the other send's and persists it with the split approve.
    #[tokio::test]
    async fn split_pair_sends_its_approve_alone_then_the_deposit() {
        let anvil = spawn_anvil(Anvil::new());
        let (wallet, contracts) = funded_relay_end(&anvil).await;
        let store = Arc::new(test_store(setup_test_db().await, ()));
        let id = UsdcRebalanceId(Uuid::new_v4());
        let witness = Arc::new(
            BroadcastWitness::new(wallet.clone(), store.clone(), id.clone()).on_deposit_signing(
                OnDepositSigning::AnotherSendFirst {
                    to: contracts.stable,
                },
            ),
        );
        let chain_wallet: TestWallet = witness.clone();
        let server = MockServer::start();
        let relay_api = MockServer::start();
        let transfer = relay_transfer(
            &server,
            &relay_api,
            wallet.clone(),
            chain_wallet,
            contracts,
            store.clone(),
        )
        .await;
        record_quoted(
            &store,
            &id,
            exact_quote(&wallet, contracts, B256::repeat_byte(0x0d)).await,
        )
        .await;
        let first_nonce = wallet
            .provider()
            .get_transaction_count(wallet.address())
            .await
            .unwrap();

        let error = transfer
            .resume_base_to_alpaca(&id, Usdc::new(float!(100)), ROBINHOOD_RELAY)
            .await
            .unwrap_err();
        assert!(
            matches!(&error, UsdcTransferError::SwapPairSplit { id: split } if *split == id),
            "got {error:?}"
        );
        let state = store.load(&id).await.unwrap().unwrap();
        let UsdcRebalance::SwapQuoted { split_approves, .. } = &state else {
            panic!("expected SwapQuoted, got {state:?}");
        };
        let [approve] = split_approves.as_slice() else {
            panic!("expected one split approve, got {split_approves:?}");
        };
        assert_eq!(approve.nonce(), first_nonce);
        assert_eq!(witness.broadcasts(), vec![approve.tx_hash()]);

        tokio::time::timeout(
            Duration::from_secs(30),
            transfer.resume_base_to_alpaca(&id, Usdc::new(float!(100)), ROBINHOOD_RELAY),
        )
        .await
        .expect("the deposit mines")
        .unwrap();

        let state = store.load(&id).await.unwrap().unwrap();
        let UsdcRebalance::SwapDeposited { deposit_tx, .. } = state else {
            panic!("expected SwapDeposited, got {state:?}");
        };
        assert_eq!(
            witness.broadcasts(),
            vec![approve.tx_hash(), approve.tx_hash(), deposit_tx],
            "the split approve, again, then the deposit"
        );
        let persisted_at_deposit = witness.seen.lock().unwrap()[2].1.clone();
        assert_eq!(
            persisted_at_deposit,
            vec![approve.tx_hash(), deposit_tx],
            "SwapDepositPrepared holds the split approve and the deposit alone"
        );
        let deposit = wallet
            .provider()
            .get_transaction_by_hash(deposit_tx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            alloy::consensus::Transaction::nonce(&deposit),
            first_nonce + 2
        );
    }

    /// A failed pair write that committed anyway is sent; one that did not
    /// commit releases the pair's nonces for the next signing.
    #[tokio::test]
    async fn failed_pair_write_releases_nonces_only_when_not_persisted() {
        let anvil = spawn_anvil(Anvil::new());
        let (wallet, contracts) = funded_relay_end(&anvil).await;
        let pool = setup_test_db().await;
        let store = Arc::new(test_store(pool.clone(), ()));
        let server = MockServer::start();
        let relay_api = MockServer::start();
        let transfer = relay_transfer(
            &server,
            &relay_api,
            wallet.clone(),
            wallet.clone(),
            contracts,
            store.clone(),
        )
        .await;

        let committed = UsdcRebalanceId(Uuid::new_v4());
        record_quoted(
            &store,
            &committed,
            exact_quote(&wallet, contracts, B256::repeat_byte(0x0d)).await,
        )
        .await;
        let persisted = PreparedSwapDeposit {
            approve: None,
            deposit: PreparedTransaction::for_test(TxHash::repeat_byte(0xd1), 40),
        };
        store
            .send(
                &committed,
                UsdcRebalanceCommand::PrepareSwapDeposit {
                    approve: None,
                    deposit: persisted.deposit.clone(),
                },
            )
            .await
            .unwrap();
        transfer
            .hop
            .persist_swap_pair(&store, &committed, &persisted)
            .await
            .unwrap();

        let unpersisted = UsdcRebalanceId(Uuid::new_v4());
        let quote = exact_quote(&wallet, contracts, B256::repeat_byte(0x0e)).await;
        record_quoted(&store, &unpersisted, quote.clone()).await;
        let first_nonce = wallet
            .provider()
            .get_transaction_count(wallet.address())
            .await
            .unwrap();
        let PreparedSwap::Deposit(pair) = transfer
            .hop
            .bridge
            .prepare_deposit(TO_HUB, &relay_quote(&quote, true).unwrap())
            .await
            .unwrap()
        else {
            panic!("expected a signed pair");
        };
        sqlx::query(
            "CREATE TRIGGER refuse_swap_pair BEFORE INSERT ON events \
             WHEN NEW.event_type = 'UsdcRebalanceEvent::SwapDepositPrepared' \
             BEGIN SELECT RAISE(ABORT, 'refused for the test'); END",
        )
        .execute(&pool)
        .await
        .unwrap();

        transfer
            .hop
            .persist_swap_pair(&store, &unpersisted, &pair)
            .await
            .unwrap_err();

        assert_eq!(
            store
                .load(&unpersisted)
                .await
                .unwrap()
                .unwrap()
                .state_name(),
            "SwapQuoted"
        );
        let next = wallet
            .prepare_pending(contracts.stable, quote.approve.unwrap().data, "next send")
            .await
            .unwrap();
        assert_eq!(next.nonce(), first_nonce, "the pair's nonces are released");
    }
}

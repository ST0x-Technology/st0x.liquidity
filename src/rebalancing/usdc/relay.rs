//! The Relay hop of a cash transfer: one end's stable deposited into Relay's
//! depository, paid out by Relay's solver at the other end.
//!
//! Chain-to-Alpaca: a pre-flight quote, the vault withdraw, the binding quote
//! (`SwapQuoted`), the approve and deposit signed and persisted together
//! (`SwapDepositPrepared`), their broadcast, the deposit confirmed
//! (`SwapDeposited`), then one Relay status read per attempt until a proven
//! fill (`Bridged`, then the Alpaca deposit) or a proven refund. A refused
//! quote, a spent budget of reverted deposits, or a refund on the origin
//! chain returns the stable to the vault (`Redepositing` ->
//! `ReturnedToSource`).
//!
//! Alpaca-to-chain: a pre-flight quote, the USD conversion and the Alpaca
//! withdrawal to the hub, then the same swap states with the pair signed on
//! the shared Ethereum wallet under its deposit-send lock, and a proven fill
//! deposited into the chain's vault. A refund at the hub is quoted again
//! within the corridor's bounds; everything else that cannot go on holds the
//! USDC at the hub, guard held, for `transfer reconcile`.

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
    BasisPoints, FailReason, IntentStatus, QuoteAmounts, QuoteBounds, QuoteFees, QuoteRequest,
    RelayBridge, RelayBridgeError, RelayClient, RelayError, RelayOrderId, RelayQuote,
    RelayRequestId, StepTransaction,
};
use st0x_bridge::{
    BridgeDirection, HopDirection, PreparedSwap, PreparedSwapDeposit, SwapBridge, SwapPayment,
    SwapSide,
};
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
    RebalanceDirection, RedepositReason, RefundSide, SwapQuote, SwapStep, TransferRef,
    UsdcRebalance, UsdcRebalanceCommand, UsdcRebalanceId, prepared_swap_ids,
};

/// One Relay corridor's hop: the bridge that signs the deposit and proves
/// the payment, the API client that quotes, the corridor's bounds, and the
/// lock that serializes the signing of a pair on the corridor chain's wallet.
/// A pair signed on the Ethereum wallet takes the service's deposit-send lock
/// instead, which every corridor's service shares.
pub(crate) struct RelayHop<Signer> {
    bridge: RelayBridge<Signer, Signer>,
    client: RelayClient,
    bounds: RelayHopCtx,
    /// Our wallet at the Ethereum hub: the solver's payee toward the hub,
    /// the depositor from it.
    hub_wallet: Address,
    chain_send_prepare: Arc<Mutex<()>>,
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
            chain_send_prepare: Arc::new(Mutex::new(())),
        }
    }
}

/// The chain-to-Alpaca side of a Relay hop: the corridor chain is the origin.
const TO_HUB: HopDirection = HopDirection::ToHub;

/// The side of the hop a transfer in `direction` swaps on: chain-to-Alpaca
/// toward the hub, Alpaca-to-chain from it.
const fn hop_direction(direction: RebalanceDirection) -> HopDirection {
    match direction {
        RebalanceDirection::BaseToAlpaca => HopDirection::ToHub,
        RebalanceDirection::AlpacaToBase => HopDirection::FromHub,
    }
}

impl<Signer: Wallet> RelayHop<Signer> {
    /// Signs and persists the pair, or picks up the one already persisted;
    /// `None` once the deposit is confirmed. Read and signed under the
    /// origin wallet's prepare `lock`: a timed-out attempt's prepare may have
    /// persisted a pair this one must send instead of signing anew. The RPC
    /// work under the lock is bounded by `quote_max_age`, past which the
    /// quote is stale anyway, so a hung RPC cannot hold the lock forever.
    async fn prepare_swap_pair(
        self: &Arc<Self>,
        cqrs: &Store<UsdcRebalance>,
        id: &UsdcRebalanceId,
        direction: HopDirection,
        lock: Arc<Mutex<()>>,
    ) -> Result<Option<(PreparedSwapDeposit, B256)>, UsdcTransferError> {
        let _prepare = lock.lock().await;
        let deadline = Instant::now() + self.bounds.quote_max_age;

        match cqrs.load(id).await? {
            Some(UsdcRebalance::SwapQuoted {
                quote,
                split_approves,
                quoted_at,
                ..
            }) => {
                let signing = PairSigning {
                    direction,
                    deadline,
                };
                let pair = self
                    .sign_swap_pair(cqrs, id, &quote, quoted_at, &split_approves, signing)
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
                self.broadcast_split_approves(id, &split_approves, direction, deadline)
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
                | UsdcRebalance::SwapRefunded { .. }
                | UsdcRebalance::SwapEscrowUnresolved { .. }
                | UsdcRebalance::SwapFailed { .. }
                | UsdcRebalance::Redepositing { .. }
                | UsdcRebalance::ReturnedToSource { .. }
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
        PairSigning {
            direction,
            deadline,
        }: PairSigning,
    ) -> Result<PreparedSwapDeposit, UsdcTransferError> {
        self.broadcast_split_approves(id, split_approves, direction, deadline)
            .await?;

        // The attempt re-quotes an expired quote before it gets here; one
        // that expired since is left for the next attempt to re-quote.
        if quote_expired(quote, quoted_at, self.bounds.quote_max_age, Utc::now()) {
            warn!(target: "rebalance", %id, %quoted_at, deadline = %quote.deadline, max_age = ?self.bounds.quote_max_age, "Relay quote expired before its deposit was signed; not signing, the next attempt re-quotes");
            return Err(UsdcTransferError::SwapQuoteExpired {
                id: id.clone(),
                quoted_at,
                deadline: quote.deadline,
            });
        }

        // After a split the allowance may already cover the deposit: the
        // bridge reads it and signs an approve only when it falls short.
        let relay_quote = relay_quote(quote, split_approves.is_empty())?;
        let prepared = self
            .sign_before(id, relay_quote, direction, deadline)
            .await?;

        match prepared {
            PreparedSwap::Deposit(pair) => {
                self.persist_swap_pair(cqrs, id, &pair, direction).await?;
                Ok(pair)
            }
            PreparedSwap::ApproveOnly { approve } => {
                self.persist_lone_approve(cqrs, id, &approve, direction)
                    .await?;
                // Persisted: a retry sends it again if this times out.
                self.before(
                    id,
                    deadline,
                    self.bridge.broadcast_approve(direction, &approve),
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
        direction: HopDirection,
        deadline: Instant,
    ) -> Result<PreparedSwap, UsdcTransferError> {
        let hop = Arc::clone(self);
        let mut signing =
            tokio::spawn(async move { hop.bridge.prepare_deposit(direction, &quote).await });

        match tokio::time::timeout_at(deadline, &mut signing).await {
            Ok(Ok(prepared)) => Ok(prepared.map_err(Box::new)?),
            Ok(Err(join_error)) => Err(prepare_panicked(id, &join_error)),
            Err(_elapsed) => {
                let hop = Arc::clone(self);
                let late_id = id.clone();
                tokio::spawn(async move {
                    hop.discard_late_signing(&late_id, signing, direction).await;
                });
                Err(self.prepare_timed_out(id))
            }
        }
    }

    /// Releases the nonces of a pair signed after its attempt timed out.
    async fn discard_late_signing(
        &self,
        id: &UsdcRebalanceId,
        signing: JoinHandle<Result<PreparedSwap, RelayBridgeError>>,
        direction: HopDirection,
    ) {
        match signing.await {
            Ok(Ok(prepared)) => {
                warn!(target: "rebalance", %id, "Releasing the nonces of a Relay pair signed after its prepare timed out");
                self.bridge.discard_prepared(direction, &prepared).await;
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
        direction: HopDirection,
        deadline: Instant,
    ) -> Result<(), UsdcTransferError> {
        self.before(id, deadline, async {
            for approve in split_approves {
                self.bridge
                    .broadcast_approve(direction, approve)
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
        direction: HopDirection,
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
                    .discard_prepared(direction, &PreparedSwap::Deposit(pair.clone()))
                    .await;
                Err(error.into())
            }
            reload => {
                let reload = reload.map(|state| state.map(|state| state.state_name()));
                error!(target: "operational_alert", alert = true, %id, deposit = %pair.deposit.tx_hash(), ?reload, ?direction, "Cannot tell whether a signed Relay pair was persisted; its nonces stay reserved and later sends from the origin wallet wait behind them until a restart");
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
        direction: HopDirection,
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
                        direction,
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

            Some(UsdcRebalance::SwapDeposited { direction, .. }) => {
                Self::require_base_to_alpaca(id, direction)?;
                self.read_relay_payment(id).await
            }

            Some(UsdcRebalance::SwapRefunded {
                direction,
                side: RefundSide::Origin,
                ..
            }) => {
                Self::require_base_to_alpaca(id, direction)?;
                self.redeposit(id, RedepositReason::Refunded).await
            }

            Some(UsdcRebalance::SwapRefunded {
                direction,
                side: RefundSide::Destination,
                refund_tx,
                amount_refunded,
                ..
            }) => {
                Self::require_base_to_alpaca(id, direction)?;
                warn!(target: "rebalance", %id, %refund_tx, %amount_refunded, "Relay refunded the deposit in USDC at the hub; the transfer holds its guard for the operator to move it and reconcile");
                Ok(())
            }

            Some(UsdcRebalance::SwapEscrowUnresolved {
                direction,
                deposit_tx,
                unresolved_at,
                ..
            }) => {
                Self::require_base_to_alpaca(id, direction)?;
                info!(target: "rebalance", %id, %deposit_tx, %unresolved_at, "Reading Relay's status of a deposit unresolved past its fill window");
                self.read_relay_payment(id).await
            }

            Some(UsdcRebalance::Redepositing { direction, .. }) => {
                Self::require_base_to_alpaca(id, direction)?;
                self.finish_redeposit(id).await
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
                | UsdcRebalance::ConversionFailed { .. }
                | UsdcRebalance::SwapFailed { .. },
            ) => Err(UsdcTransferError::PreviouslyFailedAggregate { id: id.clone() }),

            Some(UsdcRebalance::ReturnedToSource { .. } | UsdcRebalance::Reconciled { .. }) => {
                Ok(())
            }

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

    /// Drives an Alpaca-to-chain transfer on a Relay corridor from its
    /// recorded state. A state the Relay hop never reaches from the hub is
    /// refused.
    pub(crate) async fn resume_alpaca_to_chain_via_relay(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
        corridor: UsdcCorridor,
    ) -> Result<(), UsdcTransferError> {
        let state = self.cqrs.load(id).await?;
        self.require_served_corridor(id, corridor, state.as_ref())?;

        info!(target: "rebalance", ?state, "Resuming Alpaca->chain transfer over Relay");

        if let Some(state) = &state
            && state.direction() == RebalanceDirection::BaseToAlpaca
        {
            return Err(UsdcTransferError::ResumeDirectionMismatch {
                id: id.clone(),
                direction: RebalanceDirection::BaseToAlpaca,
            });
        }

        match state {
            None => self.execute_alpaca_to_chain_via_relay(id, amount).await,

            // Conversion placement fails fast, so a resume here means a crash
            // between `InitiateConversion` and its outcome: the broker order
            // id is not persisted, so it fails for reconciliation, as on CCTP.
            Some(UsdcRebalance::Converting { .. }) => {
                self.fail_conversion(
                    id,
                    "resume from Converting state requires manual reconciliation: broker \
                     order ID not persisted"
                        .into(),
                )
                .await?;
                Err(UsdcTransferError::ResumeIndeterminateConversion { id: id.clone() })
            }

            Some(UsdcRebalance::ConversionComplete { conversion, .. }) => {
                let converted = conversion.received_amount;
                let transfer = self.initiate_alpaca_withdrawal(id, converted).await?;
                let withdrawal_tx = self
                    .poll_and_confirm_withdrawal(id, &transfer.id, Utc::now())
                    .await?;
                self.quote_swap_from_hub(id, converted, Some(withdrawal_tx), Utc::now())
                    .await
            }

            Some(UsdcRebalance::Withdrawing {
                amount,
                withdrawal_ref,
                initiated_at,
                ..
            }) => {
                let TransferRef::AlpacaId(transfer_id) = withdrawal_ref else {
                    return Err(UsdcTransferError::WithdrawalRefMustBeAlpacaId { id: id.clone() });
                };
                let withdrawal_tx = self
                    .poll_and_confirm_withdrawal(id, &transfer_id, initiated_at)
                    .await?;
                self.quote_swap_from_hub(id, amount, Some(withdrawal_tx), Utc::now())
                    .await
            }

            Some(UsdcRebalance::WithdrawalComplete {
                amount,
                withdrawal_tx,
                confirmed_at,
                ..
            }) => {
                self.quote_swap_from_hub(id, amount, withdrawal_tx, confirmed_at)
                    .await
            }

            Some(UsdcRebalance::SwapQuoted { .. } | UsdcRebalance::SwapDepositPrepared { .. }) => {
                self.send_swap_deposit(id).await
            }

            Some(
                UsdcRebalance::SwapDeposited { .. } | UsdcRebalance::SwapEscrowUnresolved { .. },
            ) => self.read_relay_payment(id).await,

            Some(UsdcRebalance::SwapRefunded {
                side: RefundSide::Origin,
                ..
            }) => self.requote_refund_toward_chain(id).await,

            Some(UsdcRebalance::SwapRefunded {
                side: RefundSide::Destination,
                refund_tx,
                amount_refunded,
                ..
            }) => {
                warn!(target: "rebalance", %id, %refund_tx, %amount_refunded, "Relay refunded the deposit in the chain's stable to the chain wallet; the transfer holds its guard for the operator to move it and reconcile");
                Ok(())
            }

            Some(UsdcRebalance::Bridged {
                amount_received, ..
            }) => {
                self.continue_alpaca_to_base_from_bridged(id, amount_received)
                    .await
            }

            Some(UsdcRebalance::DepositInitiated { deposit_ref, .. }) => {
                self.resume_alpaca_to_base_deposit_initiated(id, deposit_ref)
                    .await
            }

            Some(UsdcRebalance::DepositConfirmed { .. } | UsdcRebalance::Reconciled { .. }) => {
                Ok(())
            }

            Some(
                UsdcRebalance::WithdrawalFailed { .. }
                | UsdcRebalance::BridgingFailed { .. }
                | UsdcRebalance::DepositFailed { .. }
                | UsdcRebalance::ConversionFailed { .. }
                | UsdcRebalance::SwapFailed { .. },
            ) => Err(UsdcTransferError::PreviouslyFailedAggregate { id: id.clone() }),

            Some(
                state @ (UsdcRebalance::WithdrawalSubmitting { .. }
                | UsdcRebalance::BridgingSubmitting { .. }
                | UsdcRebalance::Bridging { .. }
                | UsdcRebalance::AwaitingAttestation { .. }
                | UsdcRebalance::Attested { .. }
                | UsdcRebalance::Redepositing { .. }
                | UsdcRebalance::ReturnedToSource { .. }),
            ) => Err(UsdcTransferError::StateOffHop {
                id: id.clone(),
                state: state.state_name(),
                hop: HopKind::Relay,
            }),
        }
    }

    /// A fresh Alpaca-to-chain transfer. The pre-flight quote runs before the
    /// USD conversion, so a refusal moves nothing.
    async fn execute_alpaca_to_chain_via_relay(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
    ) -> Result<(), UsdcTransferError> {
        self.gas_readiness
            .ensure_ready(TransferGasRoute::Usdc)
            .await?;

        let preflight = self
            .accepted_quote(id, usdc_to_u256(amount)?, HopDirection::FromHub)
            .await?;
        info!(target: "rebalance", %id, %amount, request_id = %preflight.request_id, "Relay pre-flight quote from the hub accepted");

        let converted = self.execute_usd_to_usdc_conversion(id, amount).await?;
        let transfer = self.initiate_alpaca_withdrawal(id, converted).await?;
        let withdrawal_tx = self
            .poll_and_confirm_withdrawal(id, &transfer.id, Utc::now())
            .await?;
        self.quote_swap_from_hub(id, converted, Some(withdrawal_tx), Utc::now())
            .await
    }

    /// Records the binding quote for what the Alpaca withdrawal tx credited
    /// at the hub, and sends the deposit. A refused quote pages and holds the
    /// USDC at the hub, guard held at `WithdrawalComplete`: the retry quotes
    /// again, or the operator fails and reconciles it. A transient failure is
    /// left for the retry.
    async fn quote_swap_from_hub(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
        withdrawal_tx: Option<TxHash>,
        confirmed_at: DateTime<Utc>,
    ) -> Result<(), UsdcTransferError> {
        let (credited, withdrawal_tx) = self
            .credited_alpaca_withdrawal(id, amount, withdrawal_tx, confirmed_at)
            .await?;

        let quote = match self
            .binding_quote(id, credited, HopDirection::FromHub)
            .await
        {
            Ok(quote) => quote,
            Err(error) if quote_refused(&error) => {
                error!(target: "operational_alert", alert = true, %id, %withdrawal_tx, %error, "Relay refused the binding quote from the hub after the Alpaca withdrawal; the transfer holds its guard at WithdrawalComplete with the USDC at the hub until it is resumed, or failed and reconciled");
                return Ok(());
            }
            Err(error) => {
                warn!(target: "rebalance", %id, %error, "Binding Relay quote from the hub failed transiently; the transfer holds its guard at WithdrawalComplete for the retry");
                return Err(error);
            }
        };

        self.cqrs
            .send(
                id,
                UsdcRebalanceCommand::QuoteSwap {
                    quote: Box::new(quote),
                },
            )
            .await?;

        self.send_swap_deposit(id).await
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
        let preflight = self.accepted_quote(id, amount_u256, TO_HUB).await?;
        info!(target: "rebalance", %id, %amount, request_id = %preflight.request_id, "Relay pre-flight quote accepted");

        self.withdraw_from_vault(id, amount, amount_u256).await?;
        self.quote_swap_after_withdrawal(id, amount).await
    }

    /// Records the binding quote on `WithdrawalComplete` and sends the
    /// deposit. A refused quote returns the withdrawn stable to the vault; a
    /// transient failure leaves the transfer at `WithdrawalComplete`, guard
    /// held, for the retry.
    async fn quote_swap_after_withdrawal(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
    ) -> Result<(), UsdcTransferError> {
        let quote = match self.binding_quote(id, usdc_to_u256(amount)?, TO_HUB).await {
            Ok(quote) => quote,
            Err(error) if quote_refused(&error) => {
                warn!(target: "rebalance", %id, %error, "Binding Relay quote refused after the vault withdrawal; returning the stable to the vault");
                return self.redeposit(id, RedepositReason::QuoteRefused).await;
            }
            Err(error) => {
                warn!(target: "rebalance", %id, %error, "Binding Relay quote failed transiently; the transfer holds its guard at WithdrawalComplete for the retry");
                return Err(error);
            }
        };

        self.cqrs
            .send(
                id,
                UsdcRebalanceCommand::QuoteSwap {
                    quote: Box::new(quote),
                },
            )
            .await?;

        self.send_swap_deposit(id).await
    }

    /// An accepted quote for `amount_in` on the `direction` side, in its
    /// persisted form, with the origin chain's head read before it.
    async fn binding_quote(
        &self,
        id: &UsdcRebalanceId,
        amount_in: U256,
        direction: HopDirection,
    ) -> Result<SwapQuote, UsdcTransferError> {
        let origin_from_block = self
            .hop
            .bridge
            .origin_block(direction)
            .await
            .map_err(Box::new)?;
        let quote = self.accepted_quote(id, amount_in, direction).await?;

        Ok(swap_quote(
            &quote,
            self.hop.bounds.slippage_bps,
            origin_from_block,
        ))
    }

    /// Readies a `SwapQuoted` for signing: past the corridor's budget of
    /// reverted deposits the stable goes back to the vault (or, from the hub,
    /// the transfer holds); an expired quote, or one whose order id a deposit
    /// was already signed for, is replaced, and a refused replacement returns
    /// the stable to the vault (or holds). `true` when the attempt ends here.
    async fn requote_or_redeposit(&self, id: &UsdcRebalanceId) -> Result<bool, UsdcTransferError> {
        let Some(UsdcRebalance::SwapQuoted {
            direction,
            quote,
            quoted_at,
            signed_order_ids,
            split_approves,
            deposit_reverts,
            ..
        }) = self.cqrs.load(id).await?
        else {
            return Ok(false);
        };

        let max_reverts = self.hop.bounds.max_deposit_revert_redrives;
        if deposit_reverts >= max_reverts {
            match direction {
                RebalanceDirection::BaseToAlpaca => {
                    warn!(target: "rebalance", %id, deposit_reverts, max_reverts, "Relay deposits kept reverting; returning the stable to the vault");
                    self.redeposit_after_split_approves(
                        id,
                        &split_approves,
                        RedepositReason::DepositRevertsExhausted,
                    )
                    .await?;
                }
                RebalanceDirection::AlpacaToBase => {
                    error!(target: "operational_alert", alert = true, %id, deposit_reverts, max_reverts, "Relay deposits from the hub kept reverting; the transfer holds its guard at SwapQuoted with the USDC at the hub until it is moved by hand and reconciled");
                }
            }
            return Ok(true);
        }

        let signed = signed_order_ids.contains(&quote.order_id);
        let expired = quote_expired(&quote, quoted_at, self.hop.bounds.quote_max_age, Utc::now());
        if !signed && !expired {
            return Ok(false);
        }

        info!(target: "rebalance", %id, signed, expired, order_id = %quote.order_id, "Re-quoting the Relay swap before signing");
        match self
            .binding_quote(id, quote.amount_in, hop_direction(direction))
            .await
        {
            Ok(requoted) => {
                self.cqrs
                    .send(
                        id,
                        UsdcRebalanceCommand::RequoteSwap {
                            quote: Box::new(requoted),
                        },
                    )
                    .await?;
                Ok(false)
            }
            Err(error) if quote_refused(&error) => {
                match direction {
                    RebalanceDirection::BaseToAlpaca => {
                        warn!(target: "rebalance", %id, %error, "Relay refused the re-quote; returning the stable to the vault");
                        self.redeposit_after_split_approves(
                            id,
                            &split_approves,
                            RedepositReason::QuoteRefused,
                        )
                        .await?;
                    }
                    RebalanceDirection::AlpacaToBase => {
                        error!(target: "operational_alert", alert = true, %id, %error, "Relay refused the re-quote from the hub; the transfer holds its guard at SwapQuoted with the USDC at the hub until it is resumed, or moved by hand and reconciled");
                    }
                }
                Ok(true)
            }
            Err(error) => {
                warn!(target: "rebalance", %id, %error, "Relay re-quote failed transiently; the transfer holds its guard at SwapQuoted for the retry");
                Err(error)
            }
        }
    }

    /// A quote for `amount_in` of the origin stable on the `direction` side,
    /// accepted only within the corridor's bounds. The depositor takes any
    /// refund, on either chain.
    async fn accepted_quote(
        &self,
        id: &UsdcRebalanceId,
        amount_in: U256,
        direction: HopDirection,
    ) -> Result<RelayQuote, UsdcTransferError> {
        let RelayHopCtx {
            slippage_bps,
            max_quote_loss_bps,
            fill_timeout,
            ..
        } = self.hop.bounds;
        let chain = self.corridor.chain();
        let (origin, destination, user, recipient) = match direction {
            HopDirection::ToHub => (
                chain,
                Chain::Ethereum,
                self.market_maker_wallet,
                self.hop.hub_wallet,
            ),
            HopDirection::FromHub => (
                Chain::Ethereum,
                chain,
                self.hop.hub_wallet,
                self.market_maker_wallet,
            ),
        };

        let request = QuoteRequest {
            origin,
            destination,
            amount: amount_in,
            user,
            recipient,
            refund_to: user,
            slippage: basis_points(slippage_bps)?,
            ttl: fill_timeout,
        };
        let quote = self.hop.client.quote(&request).await.map_err(Box::new)?;

        // The leg after the hop, the Alpaca deposit or the vault deposit,
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
    /// then broadcasts it, records the confirmed deposit and reads Relay's
    /// status once.
    async fn send_swap_deposit(&self, id: &UsdcRebalanceId) -> Result<(), UsdcTransferError> {
        if self.requote_or_redeposit(id).await? {
            return Ok(());
        }

        let Some(state) = self.cqrs.load(id).await? else {
            return Err(UsdcTransferError::StateOffHop {
                id: id.clone(),
                state: "Uninitialized",
                hop: HopKind::Relay,
            });
        };
        let direction = hop_direction(state.direction());
        // The shared Ethereum wallet's credits are checked before a pair is
        // signed on it, as before a CCTP burn: a shortfall pages.
        if let UsdcRebalance::SwapQuoted {
            direction: RebalanceDirection::AlpacaToBase,
            amount,
            ..
        } = state
        {
            self.check_ethereum_credit_ledger(id, amount).await;
        }

        let Some((pair, order_id)) = self.prepare_and_persist_swap_pair(id, direction).await?
        else {
            return self.read_relay_payment(id).await;
        };

        let deposit_tx = self
            .hop
            .bridge
            .broadcast_deposit(direction, &pair)
            .await
            .map_err(Box::new)?;
        let deposit = match self
            .hop
            .bridge
            .confirm_deposit(direction, RelayOrderId(order_id), deposit_tx)
            .await
        {
            Ok(deposit) => deposit,
            Err(RelayBridgeError::DepositReverted { tx }) => {
                self.cqrs
                    .send(
                        id,
                        UsdcRebalanceCommand::RecordSwapDepositReverted { deposit_tx: tx },
                    )
                    .await?;
                warn!(target: "rebalance", %id, deposit_tx = %tx, "Relay deposit reverted and moved nothing; the next attempt re-quotes");
                return Err(UsdcTransferError::SwapDepositReverted {
                    id: id.clone(),
                    deposit_tx: tx,
                });
            }
            Err(error) => return Err(Box::new(error).into()),
        };

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
            "Relay deposit confirmed; reading Relay's status for the fill"
        );
        self.read_relay_payment(id).await
    }

    /// Reads Relay's status of a deposit once, adopts a fill or a refund only
    /// once it proves on chain, and otherwise ends the attempt: with
    /// `RelayFillPending` within the fill window, held at
    /// `SwapEscrowUnresolved` past it.
    async fn read_relay_payment(&self, id: &UsdcRebalanceId) -> Result<(), UsdcTransferError> {
        let (direction, quote, deposit_tx, wait) = match self.cqrs.load(id).await? {
            Some(UsdcRebalance::SwapDeposited {
                direction,
                quote,
                deposit_tx,
                deposited_at,
                ..
            }) => (
                direction,
                quote,
                deposit_tx,
                PaymentWait::FillWindow { deposited_at },
            ),
            Some(UsdcRebalance::SwapEscrowUnresolved {
                direction,
                quote,
                deposit_tx,
                ..
            }) => (direction, quote, deposit_tx, PaymentWait::Unresolved),
            _ => return Ok(()),
        };
        let side = hop_direction(direction);
        let order_id = RelayOrderId(quote.order_id);
        let past_window = wait.is_past(self.hop.bounds.fill_timeout, Utc::now());

        let report = match self
            .hop
            .client
            .status(RelayRequestId(quote.request_id))
            .await
        {
            Ok(report) => report,
            Err(error) => {
                warn!(target: "rebalance", %id, %error, "Relay status read failed; reading it again later");
                return self.payment_pending(id, deposit_tx, wait).await;
            }
        };

        match report.status {
            IntentStatus::Success => {
                let proof = self
                    .hop
                    .bridge
                    .verify_fill(side, order_id, quote.minimum_out, &report.txs)
                    .await;
                match Self::proven_payment(id, proof, past_window)? {
                    Some(payment) => self.adopt_fill(id, payment).await,
                    None => self.payment_pending(id, deposit_tx, wait).await,
                }
            }
            IntentStatus::Refund { reason } => {
                let proof = self
                    .hop
                    .bridge
                    .verify_refund(side, order_id, quote.amount_in, &report.txs)
                    .await;
                match Self::proven_payment(id, proof, past_window)? {
                    Some(payment) => self.adopt_refund(id, payment, reason, direction).await,
                    None => self.payment_pending(id, deposit_tx, wait).await,
                }
            }
            status @ (IntentStatus::RefundFailed { .. } | IntentStatus::Failure { .. }) => {
                self.cqrs
                    .send(
                        id,
                        UsdcRebalanceCommand::FailSwap {
                            reason: format!("Relay reported {status:?}"),
                        },
                    )
                    .await?;
                error!(target: "operational_alert", alert = true, %id, %deposit_tx, ?status, "Relay failed the swap with nothing paid back; the transfer holds its guard at SwapFailed until the funds are settled and it is reconciled");
                Ok(())
            }
            IntentStatus::Waiting
            | IntentStatus::InFlight(_)
            | IntentStatus::Filling
            | IntentStatus::Refunding { .. }
            | IntentStatus::NotIncluded
            | IntentStatus::Unknown(_) => self.payment_pending(id, deposit_tx, wait).await,
        }
    }

    /// The payment a proof established, `None` while the chain does not show
    /// it with its confirmations yet. A payment that does not prove, or that
    /// the chain still does not show past the fill window, is paged and read
    /// again slowly: the transfer holds its guard.
    fn proven_payment(
        id: &UsdcRebalanceId,
        proof: Result<SwapPayment, RelayBridgeError>,
        past_window: bool,
    ) -> Result<Option<SwapPayment>, UsdcTransferError> {
        match proof {
            Ok(payment) => Ok(Some(payment)),
            Err(
                error @ (RelayBridgeError::FillUnverified { .. }
                | RelayBridgeError::RefundUnverified { .. }),
            ) => {
                error!(target: "operational_alert", alert = true, %id, %error, "The Relay payment does not prove on chain; the transfer holds its guard");
                Err(UsdcTransferError::SwapPaymentUnverified {
                    id: id.clone(),
                    source: Box::new(error),
                })
            }
            Err(error) if past_window => {
                error!(target: "operational_alert", alert = true, %id, %error, "Relay names a payment the chain still does not show past the fill window; the transfer holds its guard");
                Err(UsdcTransferError::SwapPaymentUnverified {
                    id: id.clone(),
                    source: Box::new(error),
                })
            }
            Err(error) => {
                warn!(target: "rebalance", %id, %error, "Relay payment not provable yet; reading it again later");
                Ok(None)
            }
        }
    }

    /// Records the proven fill and continues as after any hop: the Alpaca
    /// deposit send and the USD conversion, or the vault deposit.
    async fn adopt_fill(
        &self,
        id: &UsdcRebalanceId,
        payment: SwapPayment,
    ) -> Result<(), UsdcTransferError> {
        self.cqrs
            .send(
                id,
                UsdcRebalanceCommand::ConfirmSwapFill {
                    fill_tx: payment.tx,
                    amount_received: payment.amount,
                },
            )
            .await?;
        info!(target: "rebalance", %id, fill_tx = %payment.tx, amount = %payment.amount, "Relay fill proven; moving it on");

        match self.cqrs.load(id).await? {
            Some(UsdcRebalance::Bridged {
                direction: RebalanceDirection::AlpacaToBase,
                amount_received,
                ..
            }) => {
                self.continue_alpaca_to_base_from_bridged(id, amount_received)
                    .await
            }
            Some(state) => self.resume_base_to_alpaca_past_hop(id, state).await,
            None => Err(UsdcTransferError::StateOffHop {
                id: id.clone(),
                state: "Uninitialized",
                hop: HopKind::Relay,
            }),
        }
    }

    /// Records the proven refund. Toward the hub an origin-side one goes back
    /// into the vault and a destination-side one waits at the hub for the
    /// operator; from the hub an origin-side one is quoted again within the
    /// corridor's bounds and a destination-side one waits in the chain wallet.
    async fn adopt_refund(
        &self,
        id: &UsdcRebalanceId,
        payment: SwapPayment,
        reason: Option<FailReason>,
        direction: RebalanceDirection,
    ) -> Result<(), UsdcTransferError> {
        let side = match payment.side {
            SwapSide::Origin => RefundSide::Origin,
            SwapSide::Destination => RefundSide::Destination,
        };
        self.cqrs
            .send(
                id,
                UsdcRebalanceCommand::RecordSwapRefund {
                    refund_tx: payment.tx,
                    side,
                    amount_refunded: payment.amount,
                },
            )
            .await?;

        match (direction, side) {
            (RebalanceDirection::BaseToAlpaca, RefundSide::Origin) => {
                warn!(target: "rebalance", %id, refund_tx = %payment.tx, amount = %payment.amount, ?reason, "Relay refunded the deposit on the origin chain; returning it to the vault");
                self.redeposit(id, RedepositReason::Refunded).await
            }
            (RebalanceDirection::BaseToAlpaca, RefundSide::Destination) => {
                error!(target: "operational_alert", alert = true, %id, refund_tx = %payment.tx, amount = %payment.amount, ?reason, "Relay refunded the deposit in USDC at the hub; the transfer holds its guard until the operator moves it and reconciles");
                Ok(())
            }
            (RebalanceDirection::AlpacaToBase, RefundSide::Origin) => {
                warn!(target: "rebalance", %id, refund_tx = %payment.tx, amount = %payment.amount, ?reason, "Relay refunded the deposit in USDC at the hub; quoting it again");
                self.requote_refund_toward_chain(id).await
            }
            (RebalanceDirection::AlpacaToBase, RefundSide::Destination) => {
                error!(target: "operational_alert", alert = true, %id, refund_tx = %payment.tx, amount = %payment.amount, ?reason, "Relay refunded the deposit in the chain's stable to the chain wallet, not the vault; the transfer holds its guard until the operator moves it and reconciles");
                Ok(())
            }
        }
    }

    /// Quotes again, from the hub, an Alpaca-to-chain refund paid in USDC at
    /// the hub, then signs and sends the new deposit: while fewer than
    /// `max_refund_retries` re-quotes followed a refund and the refund is at
    /// least `min_transfer`. Otherwise, or when Relay refuses the quote, the
    /// transfer holds at `SwapRefunded`, paged, for `transfer reconcile`; a
    /// transient failure is left for the retry.
    async fn requote_refund_toward_chain(
        &self,
        id: &UsdcRebalanceId,
    ) -> Result<(), UsdcTransferError> {
        let Some(UsdcRebalance::SwapRefunded {
            direction: RebalanceDirection::AlpacaToBase,
            side: RefundSide::Origin,
            amount_refunded,
            refund_requotes,
            ..
        }) = self.cqrs.load(id).await?
        else {
            warn!(target: "rebalance", %id, "No Alpaca-to-chain refund at the hub to quote again");
            return Ok(());
        };

        let RelayHopCtx {
            max_refund_retries,
            min_transfer,
            ..
        } = self.hop.bounds;
        let below_minimum = amount_refunded.lt(&min_transfer)?;
        if refund_requotes >= max_refund_retries || below_minimum {
            error!(target: "operational_alert", alert = true, %id, %amount_refunded, refund_requotes, max_refund_retries, %min_transfer, "Relay refunded an Alpaca-to-chain deposit at the hub past the corridor's re-quote budget or below its minimum; the transfer holds its guard at SwapRefunded with the USDC at the hub until it is moved by hand and reconciled");
            return Ok(());
        }

        let quote = match self
            .binding_quote(id, usdc_to_u256(amount_refunded)?, HopDirection::FromHub)
            .await
        {
            Ok(quote) => quote,
            Err(error) if quote_refused(&error) => {
                error!(target: "operational_alert", alert = true, %id, %error, %amount_refunded, "Relay refused the quote for a refund at the hub; the transfer holds its guard at SwapRefunded until it is resumed, or moved by hand and reconciled");
                return Ok(());
            }
            Err(error) => {
                warn!(target: "rebalance", %id, %error, "Relay quote for a refund at the hub failed transiently; the transfer holds its guard at SwapRefunded for the retry");
                return Err(error);
            }
        };

        self.cqrs
            .send(
                id,
                UsdcRebalanceCommand::RequoteSwap {
                    quote: Box::new(quote),
                },
            )
            .await?;
        info!(target: "rebalance", %id, %amount_refunded, refund_requote = refund_requotes + 1, "Quoted a refund at the hub again");

        // The status read in `send_swap_deposit` reaches this function, so
        // the call back is boxed.
        Box::pin(self.send_swap_deposit(id)).await
    }

    /// Ends an attempt with no payment: `RelayFillPending` within the fill
    /// window, the guard-holding `SwapEscrowUnresolved` once it has passed.
    /// An unresolved escrow is read again only on the operator's resume.
    async fn payment_pending(
        &self,
        id: &UsdcRebalanceId,
        deposit_tx: TxHash,
        wait: PaymentWait,
    ) -> Result<(), UsdcTransferError> {
        let PaymentWait::FillWindow { deposited_at } = wait else {
            error!(target: "operational_alert", alert = true, %id, %deposit_tx, "Relay deposit still unresolved past its fill window; the transfer holds its guard, and the next resume reads Relay again");
            return Ok(());
        };

        if !wait.is_past(self.hop.bounds.fill_timeout, Utc::now()) {
            return Err(UsdcTransferError::RelayFillPending {
                id: id.clone(),
                deposited_at,
            });
        }

        self.cqrs
            .send(id, UsdcRebalanceCommand::RecordSwapEscrowUnresolved)
            .await?;
        error!(target: "operational_alert", alert = true, %id, %deposit_tx, %deposited_at, fill_timeout = ?self.hop.bounds.fill_timeout, "Relay deposit has no fill and no refund past its fill window; the transfer holds its guard at SwapEscrowUnresolved");
        Ok(())
    }

    /// Sends again the approves of a `SwapQuoted` that went out alone, then
    /// returns the stable to the vault: the vault deposit takes the nonce
    /// after theirs, and a node may have dropped them.
    async fn redeposit_after_split_approves(
        &self,
        id: &UsdcRebalanceId,
        split_approves: &[PreparedTransaction],
        reason: RedepositReason,
    ) -> Result<(), UsdcTransferError> {
        let deadline = Instant::now() + self.hop.bounds.quote_max_age;
        self.hop
            .broadcast_split_approves(id, split_approves, TO_HUB, deadline)
            .await?;
        self.redeposit(id, reason).await
    }

    /// Starts returning the stable to the vault, then finishes it.
    async fn redeposit(
        &self,
        id: &UsdcRebalanceId,
        reason: RedepositReason,
    ) -> Result<(), UsdcTransferError> {
        self.cqrs
            .send(id, UsdcRebalanceCommand::BeginRedeposit { reason })
            .await?;
        self.finish_redeposit(id).await
    }

    /// Deposits the stable of a `Redepositing` into the vault, recording the
    /// tx before its receipt, and records it returned once mined.
    async fn finish_redeposit(&self, id: &UsdcRebalanceId) -> Result<(), UsdcTransferError> {
        let Some(UsdcRebalance::Redepositing {
            redeposit_amount,
            reason,
            deposit_tx,
            ..
        }) = self.cqrs.load(id).await?
        else {
            return Ok(());
        };

        let deposit_tx = if let Some(recorded) = deposit_tx {
            recorded
        } else {
            let submitted = self
                .submit_vault_deposit(usdc_to_u256(redeposit_amount)?)
                .await?;
            self.cqrs
                .send(
                    id,
                    UsdcRebalanceCommand::RecordRedeposit {
                        deposit_tx: submitted,
                    },
                )
                .await?;
            submitted
        };

        let deposit_block = self
            .confirm_vault_deposit(deposit_tx)
            .await
            .inspect_err(|error| match error {
                UsdcTransferError::Vault(vault)
                    if vault.is_transaction_dropped() || !vault.is_reconciliation_pending() =>
                {
                    error!(target: "operational_alert", alert = true, %id, %deposit_tx, %error, "The redeposit into the vault was dropped, reverted or refused; the transfer holds its guard at Redepositing until the stable is settled by hand and the transfer reconciled");
                }
                _ => {
                    warn!(target: "rebalance", %id, %deposit_tx, %error, "The redeposit into the vault is not confirmed yet; retrying");
                }
            })?;
        self.cqrs
            .send(
                id,
                UsdcRebalanceCommand::ConfirmRedeposit {
                    deposit_tx,
                    deposit_block,
                },
            )
            .await?;

        info!(target: "rebalance", %id, %deposit_tx, %redeposit_amount, ?reason, "The stable is back in the vault");
        Ok(())
    }

    /// The pair to broadcast and its order id, `None` once the deposit is
    /// confirmed. Runs on a detached task, as the Alpaca deposit send's
    /// prepare does, so a job timeout cannot drop the future between the
    /// signing and the persist: a pair signed and never persisted keeps its
    /// nonces reserved and stalls every later send from the origin wallet.
    async fn prepare_and_persist_swap_pair(
        &self,
        id: &UsdcRebalanceId,
        direction: HopDirection,
    ) -> Result<Option<(PreparedSwapDeposit, B256)>, UsdcTransferError> {
        let hop = Arc::clone(&self.hop);
        let cqrs = Arc::clone(&self.cqrs);
        let task_id = id.clone();
        // Toward the hub the corridor chain's wallet signs; from it the
        // Ethereum wallet, under the lock its deposit sends hold.
        let lock = match direction {
            HopDirection::ToHub => Arc::clone(&self.hop.chain_send_prepare),
            HopDirection::FromHub => Arc::clone(&self.deposit_send_prepare),
        };
        // `PrepareSwapDeposit` can outlive a cancelled job, so the task
        // continues the job's projection slot.
        let projection_slot =
            crate::conductor::projection_pause::projection_slot_for_detached_work().await;

        tokio::spawn(async move {
            let _projection_slot = projection_slot;
            hop.prepare_swap_pair(&cqrs, &task_id, direction, lock)
                .await
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
            | UsdcRebalance::SwapRefunded { .. }
            | UsdcRebalance::SwapEscrowUnresolved { .. }
            | UsdcRebalance::SwapFailed { .. }
            | UsdcRebalance::Redepositing { .. }
            | UsdcRebalance::ReturnedToSource { .. }
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

/// Where and by when a pair is signed: the hop side whose origin wallet
/// signs it, and the bound on the RPC work under that wallet's prepare lock.
#[derive(Clone, Copy)]
struct PairSigning {
    direction: HopDirection,
    deadline: Instant,
}

/// What a deposit with no payment adopted is waiting within.
#[derive(Clone, Copy)]
enum PaymentWait {
    /// The fill window, from the deposit's confirmation.
    FillWindow { deposited_at: DateTime<Utc> },
    /// Past the window: `SwapEscrowUnresolved`.
    Unresolved,
}

impl PaymentWait {
    /// Whether the fill window of `fill_timeout` has passed at `now`. A window
    /// end that does not fit counts as passed.
    fn is_past(self, fill_timeout: Duration, now: DateTime<Utc>) -> bool {
        match self {
            Self::FillWindow { deposited_at } => chrono::TimeDelta::from_std(fill_timeout)
                .ok()
                .and_then(|window| deposited_at.checked_add_signed(window))
                .is_none_or(|window_end| now >= window_end),
            Self::Unresolved => true,
        }
    }
}

/// Pages that a Relay pair prepare panicked: nonces it reserved and did not
/// persist are never released, so later sends from the origin wallet wait
/// behind them until a restart.
fn prepare_panicked(id: &UsdcRebalanceId, join_error: &JoinError) -> UsdcTransferError {
    error!(target: "operational_alert", alert = true, %id, %join_error, "The Relay pair prepare task panicked; nonces it reserved and did not persist stall later sends from the origin wallet until a restart");
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

/// Whether Relay definitively refused a quote: a non-transient error code, a
/// quote out of the corridor's bounds, or one that does not match the
/// request. Any other failure (transport, rate limit, a 5xx) may pass on a
/// retry, so the stable stays out of the vault for it.
fn quote_refused(error: &UsdcTransferError) -> bool {
    match error {
        UsdcTransferError::SwapQuoteOutOfBounds { .. } => true,
        UsdcTransferError::RelayApi(relay) => match relay.as_ref() {
            RelayError::QuoteRefused { code } => !code.is_transient(),
            RelayError::QuoteMismatch(_) => true,
            RelayError::Transport(_)
            | RelayError::Decode { .. }
            | RelayError::RateLimited { .. }
            | RelayError::HttpStatus { .. } => false,
        },
        _ => false,
    }
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

#[async_trait]
impl<Signer> ResumeAlpacaToBase for CrossVenueCashTransfer<Signer, RelayHop<Signer>>
where
    Signer: Wallet + Send + Sync + 'static,
{
    async fn resume_alpaca_to_base(
        &self,
        id: &UsdcRebalanceId,
        amount: Usdc,
        corridor: UsdcCorridor,
    ) -> Result<(), UsdcTransferError> {
        self.resume_alpaca_to_chain_via_relay(id, amount, corridor)
            .await
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
    /// Not built for Relay yet: every recorded state is refused before any
    /// call, the operator's deposit tx with it. A Relay `DepositFailed` is
    /// settled with `transfer reconcile`.
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
    use alloy::network::{EthereumWallet, TransactionBuilder};
    use alloy::node_bindings::Anvil;
    use alloy::primitives::{B256, Bytes, Signature};
    use alloy::providers::ext::AnvilApi as _;
    use alloy::providers::{Provider, ProviderBuilder, RootProvider};
    use alloy::rpc::types::{TransactionReceipt, TransactionRequest};
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
    use crate::test_utils::{
        AnvilRaindexChain, TestAnvilInstance, anvil_wallet, erc20_balance, setup_test_db,
        spawn_anvil,
    };

    type TestWallet = Arc<dyn Wallet<Provider = RootProvider>>;

    const ROBINHOOD_RELAY: UsdcCorridor = UsdcCorridor::HubRouted {
        chain: Chain::Robinhood,
        hop: HopKind::Relay,
    };

    sol! {
        #[sol(rpc)]
        interface MintableStable {
            function mint(address to, uint256 amount) external;
            function approve(address spender, uint256 amount) external returns (bool);
            function transferFrom(address from, address to, uint256 amount) external returns (bool);
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

    /// A CCTP mint recovery is never this service's.
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

    /// An attempt that confirmed the deposit and found no fill yet: Relay's
    /// status is not mocked, so the read fails and the fill counts as
    /// pending.
    fn assert_fill_pending(attempt: &Result<(), UsdcTransferError>) {
        let Err(UsdcTransferError::RelayFillPending { .. }) = attempt else {
            panic!("expected RelayFillPending, got {attempt:?}");
        };
    }

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

        assert_fill_pending(
            &transfer
                .resume_base_to_alpaca(&id, Usdc::new(float!(100)), ROBINHOOD_RELAY)
                .await,
        );

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

        assert_fill_pending(
            &tokio::time::timeout(
                Duration::from_secs(30),
                transfer.resume_base_to_alpaca(&id, Usdc::new(float!(100)), ROBINHOOD_RELAY),
            )
            .await
            .expect("the redrive sends the persisted pair"),
        );

        let state = store.load(&id).await.unwrap().unwrap();
        let UsdcRebalance::SwapDeposited { deposit_tx, .. } = state else {
            panic!("expected SwapDeposited, got {state:?}");
        };
        assert_eq!(deposit_tx, deposit.tx_hash());
    }

    /// A quote past its deadline, too close to it to mine in time, or older
    /// than the corridor's `quote_max_age`, is never signed: the attempt
    /// re-quotes first, and a re-quote that fails transiently leaves the
    /// transfer at `SwapQuoted` with its guard held and nothing broadcast.
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
            let requote = relay_api.mock(|when, then| {
                when.method(POST).path("/quote/v2");
                then.status(503);
            });
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
                matches!(
                    &error,
                    UsdcTransferError::RelayApi(relay)
                        if matches!(**relay, RelayError::HttpStatus { status: 503, .. })
                ),
                "got {error:?}"
            );
            assert!(requote.calls() > 0, "the expired quote is re-quoted");

            let state = store.load(&id).await.unwrap().unwrap();
            assert_eq!(state.state_name(), "SwapQuoted", "{state:?}");
            assert!(state.holds_rebalance_guard());
            assert_eq!(witness.broadcasts(), Vec::<TxHash>::new());
        }
    }

    /// A panic while the pair is signed pages an operational alert: the
    /// nonces it reserved stall the origin wallet until a restart.
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
                        && line.contains("stall later sends from the origin wallet until a restart")
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
        assert_fill_pending(&resumed.expect("the deposit mines once its approve is sent"));
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

        assert_fill_pending(
            &tokio::time::timeout(
                Duration::from_secs(30),
                transfer.resume_base_to_alpaca(&id, Usdc::new(float!(100)), ROBINHOOD_RELAY),
            )
            .await
            .expect("the deposit mines"),
        );

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
            .persist_swap_pair(&store, &committed, &persisted, TO_HUB)
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
            .persist_swap_pair(&store, &unpersisted, &pair, TO_HUB)
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

    /// A Robinhood stand-in with a Raindex orderbook and a hub stand-in, on two
    /// Anvil nodes, so a payment proof finds each tx on one chain only. The
    /// chain's stable is a Relay `MockStable` etched at the pinned USDG
    /// address, which the vault legs use, with `AMOUNT_IN` in the bot's wallet.
    struct RelayRig {
        chain: AnvilRaindexChain,
        chain_contracts: RelayEndContracts,
        hub_anvil: TestAnvilInstance,
        hub_wallet: TestWallet,
        hub_contracts: RelayEndContracts,
    }

    /// The vault the rig's transfers withdraw from and redeposit to.
    const RIG_VAULT_ID: RaindexVaultId = RaindexVaultId(B256::repeat_byte(0x0c));

    impl RelayRig {
        async fn deploy() -> Self {
            let chain = AnvilRaindexChain::deploy().await;
            let deployer = chain.deployer();
            let local = st0x_bridge::relay::deploy_relay_end(&deployer)
                .await
                .unwrap();
            let usdg = Chain::Robinhood.settlement_stable().address;
            let code = deployer.get_code_at(local.stable).await.unwrap();
            deployer.anvil_set_code(usdg, code).await.unwrap();
            MintableStable::new(usdg, &deployer)
                .mint(chain.bot, U256::from(AMOUNT_IN))
                .send()
                .await
                .unwrap()
                .watch()
                .await
                .unwrap();

            let hub_anvil = spawn_anvil(Anvil::new());
            let hub_wallet = anvil_wallet(
                hub_anvil.endpoint_url(),
                &B256::from_slice(&hub_anvil.keys()[0].to_bytes()),
            );
            let hub_contracts = st0x_bridge::relay::deploy_relay_end(&Self::signer_on(&hub_anvil))
                .await
                .unwrap();

            Self {
                chain,
                chain_contracts: RelayEndContracts {
                    stable: usdg,
                    depository: local.depository,
                },
                hub_anvil,
                hub_wallet,
                hub_contracts,
            }
        }

        /// A provider signing as Anvil account 2 on `anvil`: the solver.
        fn signer_on(anvil: &TestAnvilInstance) -> impl Provider + use<> {
            ProviderBuilder::new()
                .wallet(EthereumWallet::from(
                    PrivateKeySigner::from_slice(&anvil.keys()[2].to_bytes()).unwrap(),
                ))
                .connect_http(anvil.endpoint_url())
        }

        /// A Robinhood Relay service over the rig, with the corridor's real
        /// vault and the two ends' stand-in contracts.
        async fn transfer(
            &self,
            server: &MockServer,
            relay_api: &MockServer,
            store: Arc<Store<UsdcRebalance>>,
            bounds: RelayHopCtx,
        ) -> CrossVenueCashTransfer<TestWallet, RelayHop<TestWallet>> {
            let chain_wallet = self.chain.bot_wallet.clone();
            let bridge = RelayBridge::try_from_ctx(RelayCtx {
                chain: Chain::Robinhood,
                ethereum_wallet: self.hub_wallet.clone(),
                chain_wallet: chain_wallet.clone(),
                ethereum_confirmations: 1,
                chain_confirmations: 1,
            })
            .unwrap()
            .with_local_contracts(self.hub_contracts, self.chain_contracts);
            let client = RelayClient::new(None)
                .unwrap()
                .with_api_base(relay_api.base_url());
            let (broker, alpaca_wallet) = alpaca_services(server).await;
            let raindex = Arc::new(RaindexService::new(
                chain_wallet.clone(),
                RaindexContracts {
                    inventory: self.chain.orderbook,
                    orderbook: self.chain.orderbook,
                },
                chain_wallet.address(),
            ));

            CrossVenueCashTransfer::new(
                broker,
                Arc::new(alpaca_wallet),
                Arc::new(RelayHop::new(
                    bridge,
                    client,
                    bounds,
                    self.hub_wallet.address(),
                )),
                raindex,
                store,
                MarketMakingUsdcEndpoints::new(
                    ROBINHOOD_RELAY,
                    chain_wallet.address(),
                    RIG_VAULT_ID,
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

        /// The solver's payment of `amount` of `stable` to the bot on the
        /// node `solver` signs for: `transferFrom(solver, bot, amount)` with
        /// the order id appended, as Relay pays a fill or a refund.
        async fn solver_pays(
            solver: &impl Provider,
            payer: Address,
            stable: Address,
            bot: Address,
            amount: U256,
            order_id: B256,
        ) -> TxHash {
            let token = MintableStable::new(stable, solver);
            token
                .mint(payer, amount)
                .send()
                .await
                .unwrap()
                .watch()
                .await
                .unwrap();
            token
                .approve(payer, amount)
                .send()
                .await
                .unwrap()
                .watch()
                .await
                .unwrap();

            let mut calldata = MintableStable::transferFromCall {
                from: payer,
                to: bot,
                amount,
            }
            .abi_encode();
            calldata.extend_from_slice(order_id.as_slice());
            let request = TransactionRequest::default()
                .with_from(payer)
                .with_to(stable)
                .with_input(Bytes::from(calldata));

            solver
                .send_transaction(request)
                .await
                .unwrap()
                .get_receipt()
                .await
                .unwrap()
                .transaction_hash
        }

        async fn fill_on_hub(&self, amount: U256, order_id: B256) -> TxHash {
            Self::solver_pays(
                &Self::signer_on(&self.hub_anvil),
                self.hub_anvil.addresses()[2],
                self.hub_contracts.stable,
                self.hub_wallet.address(),
                amount,
                order_id,
            )
            .await
        }

        async fn refund_on_chain(&self, amount: U256, order_id: B256) -> TxHash {
            let solver = self.chain.deployer();
            let payer = solver.get_accounts().await.unwrap()[1];
            Self::solver_pays(
                &solver,
                payer,
                self.chain_contracts.stable,
                self.chain.bot,
                amount,
                order_id,
            )
            .await
        }

        /// The solver's fill of `amount` USDG to the bot on the chain.
        async fn fill_on_chain(&self, amount: U256, order_id: B256) -> TxHash {
            self.refund_on_chain(amount, order_id).await
        }

        /// The solver's refund of `amount` USDC to the bot at the hub.
        async fn refund_on_hub(&self, amount: U256, order_id: B256) -> TxHash {
            self.fill_on_hub(amount, order_id).await
        }

        /// Credits the hub wallet `amount` USDC, as an Alpaca withdrawal
        /// does, mines past it, and returns the tx.
        async fn withdraw_to_hub(&self, amount: U256) -> TxHash {
            let tx =
                MintableStable::new(self.hub_contracts.stable, Self::signer_on(&self.hub_anvil))
                    .mint(self.hub_wallet.address(), amount)
                    .send()
                    .await
                    .unwrap()
                    .get_receipt()
                    .await
                    .unwrap()
                    .transaction_hash;
            self.hub_wallet
                .provider()
                .anvil_mine(Some(2), None)
                .await
                .unwrap();
            tx
        }

        async fn hub_usdc(&self) -> U256 {
            erc20_balance(
                &self.hub_wallet,
                self.hub_contracts.stable,
                self.hub_wallet.address(),
            )
            .await
        }

        async fn vault_usdg(&self) -> U256 {
            let RaindexVaultId(vault_id) = RIG_VAULT_ID;
            self.chain
                .vault_balance(self.chain_contracts.stable, vault_id, 6)
                .await
        }
    }

    /// Answers every Relay status read with `body`.
    fn mock_status(relay_api: &MockServer, body: serde_json::Value) -> httpmock::Mock<'_> {
        relay_api.mock(|when, then| {
            when.method(GET).path("/intents/status/v3");
            then.status(200).json_body(body);
        })
    }

    /// Records a Robinhood Relay transfer of `AMOUNT_IN` up to `SwapDeposited`
    /// without a chain: the deposit is the persisted envelope's hash.
    async fn record_deposited(store: &Store<UsdcRebalance>, id: &UsdcRebalanceId) {
        record_quoted(
            store,
            id,
            crate::usdc_rebalance::swap_quote_for_test(
                U256::from(AMOUNT_IN),
                B256::repeat_byte(0x0d),
            ),
        )
        .await;
        let deposit = PreparedTransaction::for_test(TxHash::repeat_byte(0xd1), 7);
        for command in [
            UsdcRebalanceCommand::PrepareSwapDeposit {
                approve: None,
                deposit: deposit.clone(),
            },
            UsdcRebalanceCommand::ConfirmSwapDeposit {
                deposit_tx: deposit.tx_hash(),
                deposit_block: 1,
            },
        ] {
            store.send(id, command).await.unwrap();
        }
    }

    /// From `SwapDeposited` an attempt reads Relay's status once and, with no
    /// fill yet, ends at once for the job to queue the next one: it never
    /// waits for the fill in process.
    #[tokio::test]
    async fn relay_fill_wait_requeues_instead_of_blocking() {
        let anvil = spawn_anvil(Anvil::new());
        let key = B256::from_slice(&anvil.keys()[0].to_bytes());
        let wallet = anvil_wallet(anvil.endpoint_url(), &key);
        let server = MockServer::start();
        let relay_api = MockServer::start();
        let status = mock_status(&relay_api, json!({"status": "pending"}));
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
        record_deposited(&store, &id).await;

        let attempt = tokio::time::timeout(
            Duration::from_secs(5),
            transfer.resume_base_to_alpaca(&id, Usdc::new(float!(100)), ROBINHOOD_RELAY),
        )
        .await
        .expect("an attempt with no fill ends without waiting for it");

        assert_fill_pending(&attempt);
        status.assert_calls(1);
        let state = store.load(&id).await.unwrap().unwrap();
        assert_eq!(state.state_name(), "SwapDeposited");
        assert!(state.holds_rebalance_guard());
    }

    /// Past `fill_timeout` with no terminal status the deposit may still sit
    /// in Relay's escrow: the transfer holds its guard at
    /// `SwapEscrowUnresolved` and is never failed or released.
    #[tokio::test]
    async fn fill_window_expiry_holds_guard_unresolved() {
        let anvil = spawn_anvil(Anvil::new());
        let key = B256::from_slice(&anvil.keys()[0].to_bytes());
        let wallet = anvil_wallet(anvil.endpoint_url(), &key);
        let server = MockServer::start();
        let relay_api = MockServer::start();
        mock_status(&relay_api, json!({"status": "delayed"}));
        let store = Arc::new(test_store(setup_test_db().await, ()));
        let contracts = RelayEndContracts {
            stable: Address::repeat_byte(0x51),
            depository: Address::repeat_byte(0x52),
        };
        let transfer = relay_transfer_with_bounds(
            &server,
            &relay_api,
            wallet.clone(),
            wallet,
            contracts,
            store.clone(),
            RelayHopCtx {
                fill_timeout: Duration::from_millis(1),
                ..relay_bounds()
            },
        )
        .await;
        let id = UsdcRebalanceId(Uuid::new_v4());
        record_deposited(&store, &id).await;
        tokio::time::sleep(Duration::from_millis(20)).await;

        tokio::time::timeout(
            Duration::from_secs(5),
            transfer.resume_base_to_alpaca(&id, Usdc::new(float!(100)), ROBINHOOD_RELAY),
        )
        .await
        .expect("the attempt ends")
        .unwrap();

        let state = store.load(&id).await.unwrap().unwrap();
        assert_eq!(state.state_name(), "SwapEscrowUnresolved");
        assert!(state.holds_rebalance_guard());
        assert!(!state.is_reconcilable_failure());
    }

    /// A fill Relay names that the chain still does not show past the fill
    /// window is not taken for an unresolved escrow: the transfer stays at
    /// `SwapDeposited`, guard held, and the attempt ends paged for a slow
    /// re-read.
    #[tokio::test]
    async fn unprovable_payment_past_the_fill_window_pages_and_holds() {
        let anvil = spawn_anvil(Anvil::new());
        let key = B256::from_slice(&anvil.keys()[0].to_bytes());
        let wallet = anvil_wallet(anvil.endpoint_url(), &key);
        let server = MockServer::start();
        let relay_api = MockServer::start();
        mock_status(
            &relay_api,
            json!({"status": "success", "txHashes": [TxHash::repeat_byte(0xf9)]}),
        );
        let store = Arc::new(test_store(setup_test_db().await, ()));
        let transfer = relay_transfer_with_bounds(
            &server,
            &relay_api,
            wallet.clone(),
            wallet,
            RelayEndContracts {
                stable: Address::repeat_byte(0x51),
                depository: Address::repeat_byte(0x52),
            },
            store.clone(),
            RelayHopCtx {
                fill_timeout: Duration::from_millis(1),
                ..relay_bounds()
            },
        )
        .await;
        let id = UsdcRebalanceId(Uuid::new_v4());
        record_deposited(&store, &id).await;
        tokio::time::sleep(Duration::from_millis(20)).await;

        let error = tokio::time::timeout(
            Duration::from_secs(5),
            transfer.resume_base_to_alpaca(&id, Usdc::new(float!(100)), ROBINHOOD_RELAY),
        )
        .await
        .expect("the attempt ends")
        .unwrap_err();

        assert!(
            matches!(
                &error,
                UsdcTransferError::SwapPaymentUnverified { source, .. }
                    if matches!(**source, RelayBridgeError::TxNotFound { .. })
            ),
            "got {error:?}"
        );
        let state = store.load(&id).await.unwrap().unwrap();
        assert_eq!(state.state_name(), "SwapDeposited");
        assert!(state.holds_rebalance_guard());
    }

    /// The operator's resume of a deposit unresolved past its fill window
    /// reads Relay's status once more: still unpaid, it stays held with no
    /// redrive; failed at Relay, it becomes the reconcilable `SwapFailed`.
    #[tokio::test]
    async fn unresolved_escrow_resume_reads_relay_again() {
        let anvil = spawn_anvil(Anvil::new());
        let key = B256::from_slice(&anvil.keys()[0].to_bytes());
        let wallet = anvil_wallet(anvil.endpoint_url(), &key);
        let contracts = RelayEndContracts {
            stable: Address::repeat_byte(0x51),
            depository: Address::repeat_byte(0x52),
        };

        for (status_body, after) in [
            (json!({"status": "pending"}), "SwapEscrowUnresolved"),
            (
                json!({"status": "failure", "failReason": "SOLVER_CAPACITY_EXCEEDED"}),
                "SwapFailed",
            ),
        ] {
            let server = MockServer::start();
            let relay_api = MockServer::start();
            let status = mock_status(&relay_api, status_body);
            let store = Arc::new(test_store(setup_test_db().await, ()));
            let transfer = relay_transfer(
                &server,
                &relay_api,
                wallet.clone(),
                wallet.clone(),
                contracts,
                store.clone(),
            )
            .await;
            let id = UsdcRebalanceId(Uuid::new_v4());
            record_deposited(&store, &id).await;
            store
                .send(&id, UsdcRebalanceCommand::RecordSwapEscrowUnresolved)
                .await
                .unwrap();

            tokio::time::timeout(
                Duration::from_secs(5),
                transfer.resume_base_to_alpaca(&id, Usdc::new(float!(100)), ROBINHOOD_RELAY),
            )
            .await
            .expect("the attempt ends")
            .unwrap();

            status.assert_calls(1);
            let state = store.load(&id).await.unwrap().unwrap();
            assert_eq!(state.state_name(), after);
            assert!(state.holds_rebalance_guard());
        }
    }

    /// A refused binding quote after the vault withdrawal puts the withdrawn
    /// USDG back into the vault and releases the guard; the transfer never
    /// fails with the stable outside the vault.
    #[tokio::test]
    async fn binding_quote_refused_after_withdrawal_redeposits() {
        let rig = RelayRig::deploy().await;
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
        let transfer = rig
            .transfer(&server, &relay_api, store.clone(), relay_bounds())
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

        transfer
            .resume_base_to_alpaca(&id, Usdc::new(float!(100)), ROBINHOOD_RELAY)
            .await
            .unwrap();

        let state = store.load(&id).await.unwrap().unwrap();
        assert_eq!(state.state_name(), "ReturnedToSource", "{state:?}");
        assert!(!state.holds_rebalance_guard());
        assert_eq!(rig.vault_usdg().await, U256::from(AMOUNT_IN));
    }

    /// The operator's resume of a transfer held at `SwapQuoted` with an
    /// expired quote re-quotes, and a refused re-quote returns the USDG to
    /// the vault and releases the guard.
    #[tokio::test]
    async fn expired_quote_with_refused_requote_redeposits() {
        let rig = RelayRig::deploy().await;
        let server = MockServer::start();
        let relay_api = MockServer::start();
        let requote = relay_api.mock(|when, then| {
            when.method(POST).path("/quote/v2");
            then.status(400).json_body(json!({
                "message": "Amount is too high",
                "errorCode": "AMOUNT_TOO_HIGH",
                "requestId": "0x00"
            }));
        });
        let store = Arc::new(test_store(setup_test_db().await, ()));
        let transfer = rig
            .transfer(&server, &relay_api, store.clone(), relay_bounds())
            .await;
        let id = UsdcRebalanceId(Uuid::new_v4());
        let mut quote = exact_quote(
            &rig.chain.bot_wallet,
            rig.chain_contracts,
            B256::repeat_byte(0x0d),
        )
        .await;
        quote.deadline = Utc::now() - chrono::Duration::minutes(1);
        record_quoted(&store, &id, quote).await;

        transfer
            .resume_base_to_alpaca(&id, Usdc::new(float!(100)), ROBINHOOD_RELAY)
            .await
            .unwrap();

        requote.assert_calls(1);
        let state = store.load(&id).await.unwrap().unwrap();
        assert_eq!(state.state_name(), "ReturnedToSource", "{state:?}");
        assert!(!state.holds_rebalance_guard());
        assert_eq!(rig.vault_usdg().await, U256::from(AMOUNT_IN));
    }

    /// A redeposit from `SwapQuoted` first sends again an approve that went
    /// out alone: the vault deposit takes the nonce after it, and would wait
    /// forever behind a nonce no node holds.
    #[tokio::test]
    async fn redeposit_sends_a_split_approve_first() {
        let rig = RelayRig::deploy().await;
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
        let transfer = rig
            .transfer(&server, &relay_api, store.clone(), relay_bounds())
            .await;
        let id = UsdcRebalanceId(Uuid::new_v4());
        let mut quote = exact_quote(
            &rig.chain.bot_wallet,
            rig.chain_contracts,
            B256::repeat_byte(0x0d),
        )
        .await;
        let approve_step = quote.approve.clone().unwrap();
        quote.deadline = Utc::now() - chrono::Duration::minutes(1);
        record_quoted(&store, &id, quote).await;
        let approve = rig
            .chain
            .bot_wallet
            .prepare_pending(approve_step.to, approve_step.data, "split approve")
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

        tokio::time::timeout(
            Duration::from_secs(30),
            transfer.resume_base_to_alpaca(&id, Usdc::new(float!(100)), ROBINHOOD_RELAY),
        )
        .await
        .expect("the redeposit mines once the approve before it is sent")
        .unwrap();

        let state = store.load(&id).await.unwrap().unwrap();
        assert_eq!(state.state_name(), "ReturnedToSource", "{state:?}");
        let approved = rig
            .chain
            .bot_wallet
            .provider()
            .get_transaction_receipt(approve.tx_hash())
            .await
            .unwrap();
        assert!(approved.is_some(), "the split approve must be mined");
        assert_eq!(rig.vault_usdg().await, U256::from(AMOUNT_IN));
    }

    /// Deposits that keep reverting moved nothing: once
    /// `max_deposit_revert_redrives` of them reverted, the USDG goes back to
    /// the vault instead of a guard-freeing failure.
    #[tokio::test]
    async fn deposit_revert_budget_exhausted_redeposits() {
        let rig = RelayRig::deploy().await;
        rig.chain
            .make_always_revert(rig.chain_contracts.depository)
            .await;
        let server = MockServer::start();
        let relay_api = MockServer::start();
        let store = Arc::new(test_store(setup_test_db().await, ()));
        let transfer = rig
            .transfer(
                &server,
                &relay_api,
                store.clone(),
                RelayHopCtx {
                    max_deposit_revert_redrives: 1,
                    ..relay_bounds()
                },
            )
            .await;
        let id = UsdcRebalanceId(Uuid::new_v4());
        record_quoted(
            &store,
            &id,
            exact_quote(
                &rig.chain.bot_wallet,
                rig.chain_contracts,
                B256::repeat_byte(0x0d),
            )
            .await,
        )
        .await;

        for _ in 0..3 {
            let attempt = tokio::time::timeout(
                Duration::from_secs(30),
                transfer.resume_base_to_alpaca(&id, Usdc::new(float!(100)), ROBINHOOD_RELAY),
            )
            .await
            .expect("the attempt ends");
            if attempt.is_ok() {
                break;
            }
        }

        let state = store.load(&id).await.unwrap().unwrap();
        assert_eq!(state.state_name(), "ReturnedToSource", "{state:?}");
        assert!(!state.holds_rebalance_guard());
        assert_eq!(rig.vault_usdg().await, U256::from(AMOUNT_IN));
    }

    /// A refund Relay pays on the origin chain, proven on chain, goes back
    /// into the vault the withdrawal left, and the guard is released.
    #[tokio::test]
    async fn refund_toward_hub_returns_usdg_to_vault() {
        let rig = RelayRig::deploy().await;
        let order_id = B256::repeat_byte(0x0d);
        let refunded = U256::from(99_500_000_u64);
        let refund_tx = rig.refund_on_chain(refunded, order_id).await;
        let server = MockServer::start();
        let relay_api = MockServer::start();
        mock_status(
            &relay_api,
            json!({
                "status": "refund",
                "txHashes": [refund_tx],
                "failReason": "DEPOSITED_AMOUNT_TOO_LOW_TO_FILL"
            }),
        );
        let store = Arc::new(test_store(setup_test_db().await, ()));
        let transfer = rig
            .transfer(&server, &relay_api, store.clone(), relay_bounds())
            .await;
        let id = UsdcRebalanceId(Uuid::new_v4());
        record_quoted(
            &store,
            &id,
            exact_quote(&rig.chain.bot_wallet, rig.chain_contracts, order_id).await,
        )
        .await;

        tokio::time::timeout(
            Duration::from_secs(60),
            transfer.resume_base_to_alpaca(&id, Usdc::new(float!(100)), ROBINHOOD_RELAY),
        )
        .await
        .expect("the attempt ends")
        .unwrap();

        let state = store.load(&id).await.unwrap().unwrap();
        assert_eq!(state.state_name(), "ReturnedToSource", "{state:?}");
        assert!(!state.holds_rebalance_guard());
        assert_eq!(rig.vault_usdg().await, refunded);
    }

    /// A fill Relay names is adopted only once proven on the hub; the
    /// transfer then enters `Bridged` and sends the filled USDC to Alpaca's
    /// deposit address, as after a CCTP mint.
    #[tokio::test]
    async fn verified_fill_enters_bridged_and_sends_to_alpaca() {
        let rig = RelayRig::deploy().await;
        let order_id = B256::repeat_byte(0x0d);
        let fill_tx = rig.fill_on_hub(U256::from(AMOUNT_IN), order_id).await;
        // The pre-send scan for an unrecorded deposit send reads only blocks
        // past the fill with the hub's confirmations.
        rig.hub_wallet
            .provider()
            .anvil_mine(Some(10), None)
            .await
            .unwrap();
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{}/wallets", Uuid::nil()))
                .query_param("asset", "USDC")
                .query_param("network", "ethereum");
            then.status(200).json_body(json!({
                "asset_id": "5d0de74f-827b-41a7-9f74-9c07c08fe55f",
                "address": format!("{ALPACA_DEPOSIT_ADDRESS:#x}"),
                "created_at": "2025-08-07T08:52:40.656166Z"
            }));
        });
        // Alpaca has not seen the deposit yet, so the poll keeps waiting.
        server.mock(|when, then| {
            when.method(GET)
                .path(format!("/v1/accounts/{}/wallets/transfers", Uuid::nil()));
            then.status(200).json_body(json!([]));
        });
        let relay_api = MockServer::start();
        mock_status(
            &relay_api,
            json!({"status": "success", "inTxHashes": [], "txHashes": [fill_tx]}),
        );
        let store = Arc::new(test_store(setup_test_db().await, ()));
        let transfer = Arc::new(
            rig.transfer(&server, &relay_api, store.clone(), relay_bounds())
                .await,
        );
        let id = UsdcRebalanceId(Uuid::new_v4());
        record_quoted(
            &store,
            &id,
            exact_quote(&rig.chain.bot_wallet, rig.chain_contracts, order_id).await,
        )
        .await;

        let mut attempt = tokio::spawn({
            let transfer = Arc::clone(&transfer);
            let id = id.clone();
            async move {
                transfer
                    .resume_base_to_alpaca(&id, Usdc::new(float!(100)), ROBINHOOD_RELAY)
                    .await
            }
        });
        let reached = tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                if let Some(state @ UsdcRebalance::DepositInitiated { .. }) =
                    store.load(&id).await.unwrap()
                {
                    return state;
                }
                if attempt.is_finished() {
                    let outcome = (&mut attempt).await.unwrap();
                    panic!(
                        "the attempt ended before the deposit send with {outcome:?} at {:?}",
                        store.load(&id).await.unwrap()
                    );
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;
        attempt.abort();

        let Ok(UsdcRebalance::DepositInitiated { hop, amount, .. }) = reached else {
            panic!("expected DepositInitiated, got {:?}", store.load(&id).await);
        };
        assert_eq!(hop.destination_tx(), fill_tx);
        assert_eq!(amount, Usdc::new(float!(100)));
        assert_eq!(
            erc20_balance(
                &rig.hub_wallet,
                rig.hub_contracts.stable,
                ALPACA_DEPOSIT_ADDRESS
            )
            .await,
            U256::from(AMOUNT_IN)
        );
    }

    const ALPACA_DEPOSIT_ADDRESS: Address = Address::repeat_byte(0xa1);

    const TOWARD_CHAIN_ORDER_ID: B256 = B256::repeat_byte(0x0e);

    /// Records an Alpaca-to-Robinhood Relay transfer of 100 USDC up to its
    /// Alpaca withdrawal, credited by `withdrawal_tx`.
    async fn record_withdrawn_toward_chain(
        store: &Store<UsdcRebalance>,
        id: &UsdcRebalanceId,
        withdrawal_tx: TxHash,
    ) {
        for command in [
            UsdcRebalanceCommand::Initiate {
                direction: RebalanceDirection::AlpacaToBase,
                corridor: ROBINHOOD_RELAY,
                amount: Usdc::new(float!(100)),
                withdrawal: TransferRef::AlpacaId(st0x_execution::AlpacaTransferId::from(
                    Uuid::new_v4(),
                )),
            },
            UsdcRebalanceCommand::ConfirmWithdrawal {
                withdrawal_tx: Some(withdrawal_tx),
            },
        ] {
            store.send(id, command).await.unwrap();
        }
    }

    /// Records an Alpaca-to-Robinhood Relay transfer up to `SwapQuoted` on
    /// `quote`, signed at the hub.
    async fn record_quoted_toward_chain(
        store: &Store<UsdcRebalance>,
        id: &UsdcRebalanceId,
        quote: SwapQuote,
    ) {
        record_withdrawn_toward_chain(store, id, TxHash::repeat_byte(0x78)).await;
        store
            .send(
                id,
                UsdcRebalanceCommand::QuoteSwap {
                    quote: Box::new(quote),
                },
            )
            .await
            .unwrap();
    }

    /// Records an Alpaca-to-Robinhood Relay transfer of `AMOUNT_IN` up to
    /// `SwapDeposited` without a chain, for `order_id`.
    async fn record_deposited_toward_chain(
        store: &Store<UsdcRebalance>,
        id: &UsdcRebalanceId,
        order_id: B256,
    ) {
        record_quoted_toward_chain(
            store,
            id,
            crate::usdc_rebalance::swap_quote_for_test(U256::from(AMOUNT_IN), order_id),
        )
        .await;
        record_pair_deposited(store, id, 7).await;
    }

    /// Persists and confirms a stand-in pair at `nonce` on a `SwapQuoted`.
    async fn record_pair_deposited(store: &Store<UsdcRebalance>, id: &UsdcRebalanceId, nonce: u64) {
        let deposit = PreparedTransaction::for_test(
            TxHash::repeat_byte(0xd0 + u8::try_from(nonce).unwrap()),
            nonce,
        );
        for command in [
            UsdcRebalanceCommand::PrepareSwapDeposit {
                approve: None,
                deposit: deposit.clone(),
            },
            UsdcRebalanceCommand::ConfirmSwapDeposit {
                deposit_tx: deposit.tx_hash(),
                deposit_block: 1,
            },
        ] {
            store.send(id, command).await.unwrap();
        }
    }

    /// Answers every quote with a 503, recording what was asked.
    fn mock_quote_unavailable(relay_api: &MockServer) -> httpmock::Mock<'_> {
        relay_api.mock(|when, then| {
            when.method(POST).path("/quote/v2");
            then.status(503);
        })
    }

    /// The happy path from the hub: the pair is signed on the Ethereum
    /// wallet, the fill is proven on Robinhood, and the filled USDG goes into
    /// the corridor's vault, which clears the guard.
    #[tokio::test]
    async fn alpaca_to_robinhood_fill_deposits_into_the_vault() {
        let rig = RelayRig::deploy().await;
        rig.withdraw_to_hub(U256::from(AMOUNT_IN)).await;
        let fill_tx = rig
            .fill_on_chain(U256::from(AMOUNT_IN), TOWARD_CHAIN_ORDER_ID)
            .await;
        let server = MockServer::start();
        let relay_api = MockServer::start();
        mock_status(
            &relay_api,
            json!({"status": "success", "inTxHashes": [], "txHashes": [fill_tx]}),
        );
        let store = Arc::new(test_store(setup_test_db().await, ()));
        let transfer = rig
            .transfer(&server, &relay_api, store.clone(), relay_bounds())
            .await;
        let id = UsdcRebalanceId(Uuid::new_v4());
        record_quoted_toward_chain(
            &store,
            &id,
            exact_quote(&rig.hub_wallet, rig.hub_contracts, TOWARD_CHAIN_ORDER_ID).await,
        )
        .await;

        tokio::time::timeout(
            Duration::from_secs(60),
            transfer.resume_alpaca_to_base(&id, Usdc::new(float!(100)), ROBINHOOD_RELAY),
        )
        .await
        .expect("the attempt ends")
        .unwrap();

        let state = store.load(&id).await.unwrap().unwrap();
        let UsdcRebalance::DepositConfirmed { hop, .. } = &state else {
            panic!("expected DepositConfirmed, got {state:?}");
        };
        assert_eq!(hop.destination_tx(), fill_tx);
        assert!(!state.holds_rebalance_guard());
        assert_eq!(rig.vault_usdg().await, U256::from(AMOUNT_IN));
        assert_eq!(
            rig.hub_usdc().await,
            U256::ZERO,
            "the hub's USDC went to Relay"
        );
    }

    /// The Ethereum-origin pair is signed under the deposit-send lock every
    /// corridor's service shares: while another send holds it, nothing is
    /// signed or persisted.
    #[tokio::test]
    async fn ethereum_relay_pair_shares_nonce_lock_with_deposit_sends() {
        let rig = RelayRig::deploy().await;
        rig.withdraw_to_hub(U256::from(AMOUNT_IN)).await;
        let server = MockServer::start();
        let relay_api = MockServer::start();
        mock_status(&relay_api, json!({"status": "pending"}));
        let store = Arc::new(test_store(setup_test_db().await, ()));
        let lock = Arc::new(tokio::sync::Mutex::new(()));
        let transfer = Arc::new(
            rig.transfer(&server, &relay_api, store.clone(), relay_bounds())
                .await
                .with_deposit_send_lock(Arc::clone(&lock)),
        );
        let id = UsdcRebalanceId(Uuid::new_v4());
        record_quoted_toward_chain(
            &store,
            &id,
            exact_quote(&rig.hub_wallet, rig.hub_contracts, TOWARD_CHAIN_ORDER_ID).await,
        )
        .await;

        let deposit_send = lock.lock().await;
        let attempt = tokio::spawn({
            let transfer = Arc::clone(&transfer);
            let id = id.clone();
            async move {
                transfer
                    .resume_alpaca_to_base(&id, Usdc::new(float!(100)), ROBINHOOD_RELAY)
                    .await
            }
        });
        tokio::time::sleep(Duration::from_millis(500)).await;

        assert!(
            !attempt.is_finished(),
            "the attempt waits for the deposit-send lock"
        );
        let state = store.load(&id).await.unwrap().unwrap();
        assert_eq!(
            state.state_name(),
            "SwapQuoted",
            "nothing is signed while a deposit send holds the lock"
        );
        assert_eq!(rig.hub_usdc().await, U256::from(AMOUNT_IN));

        drop(deposit_send);
        let outcome = tokio::time::timeout(Duration::from_secs(30), attempt)
            .await
            .expect("the attempt ends once the lock is free")
            .unwrap();
        assert_fill_pending(&outcome);
        let state = store.load(&id).await.unwrap().unwrap();
        assert_eq!(state.state_name(), "SwapDeposited", "{state:?}");
        assert_eq!(rig.hub_usdc().await, U256::ZERO);
    }

    /// Before an Ethereum-origin pair is signed the shared wallet's credit
    /// ledger is checked, as before a CCTP burn: a shortfall pages and the
    /// pair is still signed.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn ethereum_relay_pair_runs_the_credit_ledger_check() {
        let rig = RelayRig::deploy().await;
        rig.withdraw_to_hub(U256::from(AMOUNT_IN)).await;
        let server = MockServer::start();
        let relay_api = MockServer::start();
        mock_status(&relay_api, json!({"status": "pending"}));
        let pool = setup_test_db().await;
        let store = Arc::new(test_store(pool.clone(), ()));
        let transfer = rig
            .transfer(&server, &relay_api, store.clone(), relay_bounds())
            .await
            .with_credit_ledger(pool);
        let other = UsdcRebalanceId(Uuid::new_v4());
        record_quoted_toward_chain(
            &store,
            &other,
            crate::usdc_rebalance::swap_quote_for_test(
                U256::from(AMOUNT_IN),
                B256::repeat_byte(0x0f),
            ),
        )
        .await;
        let id = UsdcRebalanceId(Uuid::new_v4());
        record_quoted_toward_chain(
            &store,
            &id,
            exact_quote(&rig.hub_wallet, rig.hub_contracts, TOWARD_CHAIN_ORDER_ID).await,
        )
        .await;

        let attempt = tokio::time::timeout(
            Duration::from_secs(30),
            transfer.resume_alpaca_to_base(&id, Usdc::new(float!(100)), ROBINHOOD_RELAY),
        )
        .await
        .expect("the attempt ends");

        assert_fill_pending(&attempt);
        assert!(logs_contain(
            "Ethereum wallet USDC is short of the credits of the open USDC transfers"
        ));
    }

    /// The binding quote from the hub is for what the Alpaca withdrawal tx
    /// credited, from the hub to the chain; a transient failure leaves the
    /// transfer at `WithdrawalComplete` with its guard held.
    #[tokio::test]
    async fn binding_quote_toward_chain_swaps_the_withdrawal_credit() {
        let rig = RelayRig::deploy().await;
        let withdrawal_tx = rig.withdraw_to_hub(U256::from(99_900_000_u64)).await;
        let server = MockServer::start();
        let relay_api = MockServer::start();
        let quote = relay_api.mock(|when, then| {
            when.method(POST).path("/quote/v2").json_body_includes(
                json!({
                    "originChainId": 1,
                    "destinationChainId": 4663,
                    "originCurrency": Chain::Ethereum.settlement_stable().address,
                    "destinationCurrency": Chain::Robinhood.settlement_stable().address,
                    "amount": "99900000",
                })
                .to_string(),
            );
            then.status(503);
        });
        let store = Arc::new(test_store(setup_test_db().await, ()));
        let transfer = rig
            .transfer(&server, &relay_api, store.clone(), relay_bounds())
            .await;
        let id = UsdcRebalanceId(Uuid::new_v4());
        record_withdrawn_toward_chain(&store, &id, withdrawal_tx).await;

        let error = tokio::time::timeout(
            Duration::from_secs(30),
            transfer.resume_alpaca_to_base(&id, Usdc::new(float!(100)), ROBINHOOD_RELAY),
        )
        .await
        .expect("the attempt ends")
        .unwrap_err();

        assert!(
            matches!(
                &error,
                UsdcTransferError::RelayApi(relay)
                    if matches!(**relay, RelayError::HttpStatus { status: 503, .. })
            ),
            "got {error:?}"
        );
        assert!(
            quote.calls() > 0,
            "the credited amount is quoted from the hub"
        );
        let state = store.load(&id).await.unwrap().unwrap();
        assert_eq!(state.state_name(), "WithdrawalComplete");
        assert!(state.holds_rebalance_guard());
    }

    /// A binding quote Relay refuses after the Alpaca withdrawal pages and
    /// holds the USDC at the hub with the guard held: there is no vault to
    /// return it to.
    #[tokio::test]
    async fn binding_quote_refused_toward_chain_holds_at_the_hub() {
        let rig = RelayRig::deploy().await;
        let withdrawal_tx = rig.withdraw_to_hub(U256::from(AMOUNT_IN)).await;
        let server = MockServer::start();
        let relay_api = MockServer::start();
        relay_api.mock(|when, then| {
            when.method(POST).path("/quote/v2");
            then.status(400).json_body(json!({
                "message": "Amount is too low",
                "errorCode": "AMOUNT_TOO_LOW",
                "requestId": "0x00"
            }));
        });
        let store = Arc::new(test_store(setup_test_db().await, ()));
        let transfer = rig
            .transfer(&server, &relay_api, store.clone(), relay_bounds())
            .await;
        let id = UsdcRebalanceId(Uuid::new_v4());
        record_withdrawn_toward_chain(&store, &id, withdrawal_tx).await;

        tokio::time::timeout(
            Duration::from_secs(30),
            transfer.resume_alpaca_to_base(&id, Usdc::new(float!(100)), ROBINHOOD_RELAY),
        )
        .await
        .expect("the attempt ends")
        .unwrap();

        let state = store.load(&id).await.unwrap().unwrap();
        assert_eq!(state.state_name(), "WithdrawalComplete");
        assert!(state.holds_rebalance_guard());
        assert_eq!(rig.hub_usdc().await, U256::from(AMOUNT_IN));
    }

    /// A refund paid in USDC at the hub is quoted again, for the refunded
    /// amount, from the hub to the chain.
    #[tokio::test]
    async fn refund_toward_chain_requotes_from_refunded_amount() {
        let rig = RelayRig::deploy().await;
        let refunded = U256::from(99_500_000_u64);
        let refund_tx = rig.refund_on_hub(refunded, TOWARD_CHAIN_ORDER_ID).await;
        rig.hub_wallet
            .provider()
            .anvil_mine(Some(2), None)
            .await
            .unwrap();
        let server = MockServer::start();
        let relay_api = MockServer::start();
        mock_status(
            &relay_api,
            json!({
                "status": "refund",
                "txHashes": [refund_tx],
                "failReason": "SOLVER_CAPACITY_EXCEEDED"
            }),
        );
        let requote = relay_api.mock(|when, then| {
            when.method(POST).path("/quote/v2").json_body_includes(
                json!({
                    "originChainId": 1,
                    "destinationChainId": 4663,
                    "amount": "99500000",
                })
                .to_string(),
            );
            then.status(503);
        });
        let store = Arc::new(test_store(setup_test_db().await, ()));
        let transfer = rig
            .transfer(
                &server,
                &relay_api,
                store.clone(),
                RelayHopCtx {
                    min_transfer: Usdc::new(float!(10)),
                    ..relay_bounds()
                },
            )
            .await;
        let id = UsdcRebalanceId(Uuid::new_v4());
        record_deposited_toward_chain(&store, &id, TOWARD_CHAIN_ORDER_ID).await;

        let error = tokio::time::timeout(
            Duration::from_secs(30),
            transfer.resume_alpaca_to_base(&id, Usdc::new(float!(100)), ROBINHOOD_RELAY),
        )
        .await
        .expect("the attempt ends")
        .unwrap_err();

        assert!(
            matches!(
                &error,
                UsdcTransferError::RelayApi(relay)
                    if matches!(**relay, RelayError::HttpStatus { status: 503, .. })
            ),
            "got {error:?}"
        );
        assert!(requote.calls() > 0, "the refunded amount is quoted again");
        let state = store.load(&id).await.unwrap().unwrap();
        let UsdcRebalance::SwapRefunded {
            side,
            amount_refunded,
            ..
        } = &state
        else {
            panic!("expected SwapRefunded, got {state:?}");
        };
        assert_eq!(*side, RefundSide::Origin);
        assert_eq!(*amount_refunded, Usdc::new(float!(99.5)));
        assert!(state.holds_rebalance_guard());
    }

    /// A refund at the hub below the corridor's `min_transfer` is not quoted
    /// again: the transfer holds at `SwapRefunded`, guard held, for
    /// `transfer reconcile`.
    #[tokio::test]
    async fn refund_retries_stop_at_min_transfer() {
        let anvil = spawn_anvil(Anvil::new());
        let key = B256::from_slice(&anvil.keys()[0].to_bytes());
        let wallet = anvil_wallet(anvil.endpoint_url(), &key);
        let server = MockServer::start();
        let relay_api = MockServer::start();
        let quote = mock_quote_unavailable(&relay_api);
        let store = Arc::new(test_store(setup_test_db().await, ()));
        let contracts = RelayEndContracts {
            stable: Address::repeat_byte(0x51),
            depository: Address::repeat_byte(0x52),
        };
        let transfer = relay_transfer_with_bounds(
            &server,
            &relay_api,
            wallet.clone(),
            wallet,
            contracts,
            store.clone(),
            RelayHopCtx {
                min_transfer: Usdc::new(float!(99.6)),
                ..relay_bounds()
            },
        )
        .await;
        let id = UsdcRebalanceId(Uuid::new_v4());
        record_deposited_toward_chain(&store, &id, TOWARD_CHAIN_ORDER_ID).await;
        store
            .send(
                &id,
                UsdcRebalanceCommand::RecordSwapRefund {
                    refund_tx: TxHash::repeat_byte(0xe1),
                    side: RefundSide::Origin,
                    amount_refunded: U256::from(99_500_000_u64),
                },
            )
            .await
            .unwrap();

        transfer
            .resume_alpaca_to_base(&id, Usdc::new(float!(100)), ROBINHOOD_RELAY)
            .await
            .unwrap();

        quote.assert_calls(0);
        let state = store.load(&id).await.unwrap().unwrap();
        assert_eq!(state.state_name(), "SwapRefunded");
        assert!(state.holds_rebalance_guard());
        assert!(state.is_reconcilable_failure());
    }

    /// After `max_refund_retries` re-quotes that followed a refund, the next
    /// refund at the hub holds at `SwapRefunded` instead of quoting again.
    #[tokio::test]
    async fn refund_retries_stop_at_the_retry_budget() {
        let anvil = spawn_anvil(Anvil::new());
        let key = B256::from_slice(&anvil.keys()[0].to_bytes());
        let wallet = anvil_wallet(anvil.endpoint_url(), &key);
        let server = MockServer::start();
        let relay_api = MockServer::start();
        let quote = mock_quote_unavailable(&relay_api);
        let store = Arc::new(test_store(setup_test_db().await, ()));
        let contracts = RelayEndContracts {
            stable: Address::repeat_byte(0x51),
            depository: Address::repeat_byte(0x52),
        };
        let transfer = relay_transfer_with_bounds(
            &server,
            &relay_api,
            wallet.clone(),
            wallet,
            contracts,
            store.clone(),
            RelayHopCtx {
                min_transfer: Usdc::new(float!(10)),
                max_refund_retries: 1,
                ..relay_bounds()
            },
        )
        .await;
        let id = UsdcRebalanceId(Uuid::new_v4());
        record_deposited_toward_chain(&store, &id, TOWARD_CHAIN_ORDER_ID).await;
        let refund = |amount: u64| UsdcRebalanceCommand::RecordSwapRefund {
            refund_tx: TxHash::repeat_byte(0xe1),
            side: RefundSide::Origin,
            amount_refunded: U256::from(amount),
        };
        store.send(&id, refund(99_500_000)).await.unwrap();
        store
            .send(
                &id,
                UsdcRebalanceCommand::RequoteSwap {
                    quote: Box::new(crate::usdc_rebalance::swap_quote_for_test(
                        U256::from(99_500_000_u64),
                        B256::repeat_byte(0x1e),
                    )),
                },
            )
            .await
            .unwrap();
        record_pair_deposited(&store, &id, 8).await;
        store.send(&id, refund(99_000_000)).await.unwrap();

        transfer
            .resume_alpaca_to_base(&id, Usdc::new(float!(100)), ROBINHOOD_RELAY)
            .await
            .unwrap();

        quote.assert_calls(0);
        let state = store.load(&id).await.unwrap().unwrap();
        let UsdcRebalance::SwapRefunded {
            signed_order_ids, ..
        } = &state
        else {
            panic!("expected SwapRefunded, got {state:?}");
        };
        assert_eq!(
            *signed_order_ids,
            vec![TOWARD_CHAIN_ORDER_ID, B256::repeat_byte(0x1e)],
            "every order id signed stays on the record"
        );
        assert!(state.holds_rebalance_guard());
        assert!(state.is_reconcilable_failure());
    }

    /// Past `max_deposit_revert_redrives` reverted deposits from the hub the
    /// transfer holds at `SwapQuoted` with the USDC at the hub, guard held,
    /// for `transfer reconcile`: nothing is redeposited or re-quoted.
    #[tokio::test]
    async fn deposit_revert_budget_exhausted_toward_chain_holds_at_the_hub() {
        let rig = RelayRig::deploy().await;
        rig.withdraw_to_hub(U256::from(AMOUNT_IN)).await;
        rig.hub_wallet
            .provider()
            .anvil_set_code(
                rig.hub_contracts.depository,
                alloy::primitives::bytes!("5f5ffd"),
            )
            .await
            .unwrap();
        let server = MockServer::start();
        let relay_api = MockServer::start();
        let quote = mock_quote_unavailable(&relay_api);
        let store = Arc::new(test_store(setup_test_db().await, ()));
        let transfer = rig
            .transfer(
                &server,
                &relay_api,
                store.clone(),
                RelayHopCtx {
                    max_deposit_revert_redrives: 1,
                    ..relay_bounds()
                },
            )
            .await;
        let id = UsdcRebalanceId(Uuid::new_v4());
        record_quoted_toward_chain(
            &store,
            &id,
            exact_quote(&rig.hub_wallet, rig.hub_contracts, TOWARD_CHAIN_ORDER_ID).await,
        )
        .await;

        let mut held = false;
        for _ in 0..3 {
            let attempt = tokio::time::timeout(
                Duration::from_secs(30),
                transfer.resume_alpaca_to_base(&id, Usdc::new(float!(100)), ROBINHOOD_RELAY),
            )
            .await
            .expect("the attempt ends");
            if attempt.is_ok() {
                held = true;
                break;
            }
        }

        assert!(held, "the spent revert budget ends the attempt with a hold");
        quote.assert_calls(0);
        let state = store.load(&id).await.unwrap().unwrap();
        let UsdcRebalance::SwapQuoted {
            deposit_reverts, ..
        } = &state
        else {
            panic!("expected SwapQuoted, got {state:?}");
        };
        assert_eq!(*deposit_reverts, 1, "one deposit reverted");
        assert!(state.holds_rebalance_guard());
        assert!(state.is_reconcilable_failure());
        assert_eq!(rig.hub_usdc().await, U256::from(AMOUNT_IN));
    }

    /// Answers every quote with Relay's refusal of the amount.
    fn mock_quote_refused(relay_api: &MockServer) -> httpmock::Mock<'_> {
        relay_api.mock(|when, then| {
            when.method(POST).path("/quote/v2");
            then.status(400).json_body(json!({
                "message": "Amount is too low",
                "errorCode": "AMOUNT_TOO_LOW",
                "requestId": "0x00"
            }));
        })
    }

    /// A refused pre-flight quote from the hub ends a fresh attempt before
    /// the USD conversion: nothing is recorded and no order is placed.
    #[tokio::test]
    async fn preflight_quote_refused_toward_chain_moves_nothing() {
        let anvil = spawn_anvil(Anvil::new());
        let key = B256::from_slice(&anvil.keys()[0].to_bytes());
        let wallet = anvil_wallet(anvil.endpoint_url(), &key);
        let server = MockServer::start();
        let orders = server.mock(|when, then| {
            when.method(POST).path_includes("/orders");
            then.status(500);
        });
        let relay_api = MockServer::start();
        let quote = mock_quote_refused(&relay_api);
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
            .resume_alpaca_to_base(&id, Usdc::new(float!(1000)), ROBINHOOD_RELAY)
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
        orders.assert_calls(0);
        assert_eq!(store.load(&id).await.unwrap(), None);
    }

    /// An expired quote from the hub that Relay refuses to replace holds the
    /// transfer at `SwapQuoted` with the USDC at the hub, guard held: there
    /// is no vault to return it to.
    #[tokio::test]
    async fn refused_requote_toward_chain_holds_at_the_hub() {
        let rig = RelayRig::deploy().await;
        rig.withdraw_to_hub(U256::from(AMOUNT_IN)).await;
        let server = MockServer::start();
        let relay_api = MockServer::start();
        let quote = mock_quote_refused(&relay_api);
        let store = Arc::new(test_store(setup_test_db().await, ()));
        let transfer = rig
            .transfer(&server, &relay_api, store.clone(), relay_bounds())
            .await;
        let id = UsdcRebalanceId(Uuid::new_v4());
        let mut expired =
            exact_quote(&rig.hub_wallet, rig.hub_contracts, TOWARD_CHAIN_ORDER_ID).await;
        expired.deadline = Utc::now() - chrono::Duration::minutes(1);
        record_quoted_toward_chain(&store, &id, expired).await;

        tokio::time::timeout(
            Duration::from_secs(30),
            transfer.resume_alpaca_to_base(&id, Usdc::new(float!(100)), ROBINHOOD_RELAY),
        )
        .await
        .expect("the attempt ends")
        .unwrap();

        assert!(quote.calls() > 0, "the expired quote is quoted again");
        let state = store.load(&id).await.unwrap().unwrap();
        assert_eq!(state.state_name(), "SwapQuoted", "{state:?}");
        assert!(state.holds_rebalance_guard());
        assert!(state.is_reconcilable_failure());
        assert_eq!(rig.hub_usdc().await, U256::from(AMOUNT_IN));
    }

    /// A refund at the hub whose new quote Relay refuses holds at
    /// `SwapRefunded`, guard held, for `transfer reconcile`.
    #[tokio::test]
    async fn refused_refund_requote_toward_chain_holds_at_the_hub() {
        let anvil = spawn_anvil(Anvil::new());
        let key = B256::from_slice(&anvil.keys()[0].to_bytes());
        let wallet = anvil_wallet(anvil.endpoint_url(), &key);
        let server = MockServer::start();
        let relay_api = MockServer::start();
        let quote = mock_quote_refused(&relay_api);
        let store = Arc::new(test_store(setup_test_db().await, ()));
        let contracts = RelayEndContracts {
            stable: Address::repeat_byte(0x51),
            depository: Address::repeat_byte(0x52),
        };
        let transfer = relay_transfer_with_bounds(
            &server,
            &relay_api,
            wallet.clone(),
            wallet,
            contracts,
            store.clone(),
            RelayHopCtx {
                min_transfer: Usdc::new(float!(10)),
                ..relay_bounds()
            },
        )
        .await;
        let id = UsdcRebalanceId(Uuid::new_v4());
        record_deposited_toward_chain(&store, &id, TOWARD_CHAIN_ORDER_ID).await;
        store
            .send(
                &id,
                UsdcRebalanceCommand::RecordSwapRefund {
                    refund_tx: TxHash::repeat_byte(0xe1),
                    side: RefundSide::Origin,
                    amount_refunded: U256::from(99_500_000_u64),
                },
            )
            .await
            .unwrap();

        transfer
            .resume_alpaca_to_base(&id, Usdc::new(float!(100)), ROBINHOOD_RELAY)
            .await
            .unwrap();

        assert!(quote.calls() > 0, "the refund is quoted again");
        let state = store.load(&id).await.unwrap().unwrap();
        assert_eq!(state.state_name(), "SwapRefunded", "{state:?}");
        assert!(state.holds_rebalance_guard());
        assert!(state.is_reconcilable_failure());
    }

    /// A refund Relay pays in USDG to the chain wallet, proven on the chain,
    /// holds at `SwapRefunded` with the guard held: the stable is in the
    /// wallet, not the vault, and nothing is quoted again.
    #[tokio::test]
    async fn destination_refund_toward_chain_holds_in_the_chain_wallet() {
        let rig = RelayRig::deploy().await;
        let refunded = U256::from(99_500_000_u64);
        let refund_tx = rig.refund_on_chain(refunded, TOWARD_CHAIN_ORDER_ID).await;
        let server = MockServer::start();
        let relay_api = MockServer::start();
        let quote = mock_quote_unavailable(&relay_api);
        mock_status(
            &relay_api,
            json!({
                "status": "refund",
                "txHashes": [refund_tx],
                "failReason": "SOLVER_CAPACITY_EXCEEDED"
            }),
        );
        let store = Arc::new(test_store(setup_test_db().await, ()));
        let transfer = rig
            .transfer(&server, &relay_api, store.clone(), relay_bounds())
            .await;
        let id = UsdcRebalanceId(Uuid::new_v4());
        record_deposited_toward_chain(&store, &id, TOWARD_CHAIN_ORDER_ID).await;

        tokio::time::timeout(
            Duration::from_secs(30),
            transfer.resume_alpaca_to_base(&id, Usdc::new(float!(100)), ROBINHOOD_RELAY),
        )
        .await
        .expect("the attempt ends")
        .unwrap();

        quote.assert_calls(0);
        let state = store.load(&id).await.unwrap().unwrap();
        let UsdcRebalance::SwapRefunded { side, .. } = &state else {
            panic!("expected SwapRefunded, got {state:?}");
        };
        assert_eq!(*side, RefundSide::Destination);
        assert!(state.holds_rebalance_guard());
        assert!(state.is_reconcilable_failure());
        assert_eq!(rig.vault_usdg().await, U256::ZERO);
    }

    /// A pair, or a lone approve, signed on the hub for a transfer the
    /// operator reconciled meanwhile can never be persisted or sent: its
    /// nonces are released, so later sends from the shared Ethereum wallet do
    /// not wait behind them.
    #[tokio::test]
    async fn envelopes_signed_for_a_reconciled_transfer_release_their_nonces() {
        let anvil = spawn_anvil(Anvil::new());
        let (wallet, contracts) = funded_relay_end(&anvil).await;
        let store = Arc::new(test_store(setup_test_db().await, ()));
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
        let id = UsdcRebalanceId(Uuid::new_v4());
        let quote = exact_quote(&wallet, contracts, TOWARD_CHAIN_ORDER_ID).await;
        record_quoted_toward_chain(&store, &id, quote.clone()).await;
        store
            .send(
                &id,
                UsdcRebalanceCommand::ReconcileStuckRebalance {
                    reason: crate::usdc_rebalance::ReconcileReason::FundsMovedManually,
                },
            )
            .await
            .unwrap();
        let first_nonce = wallet
            .provider()
            .get_transaction_count(wallet.address())
            .await
            .unwrap();

        let PreparedSwap::Deposit(pair) = transfer
            .hop
            .bridge
            .prepare_deposit(HopDirection::FromHub, &relay_quote(&quote, true).unwrap())
            .await
            .unwrap()
        else {
            panic!("expected a signed pair");
        };
        transfer
            .hop
            .persist_swap_pair(&store, &id, &pair, HopDirection::FromHub)
            .await
            .unwrap_err();
        let next = wallet
            .prepare_pending(
                contracts.stable,
                quote.approve.clone().unwrap().data,
                "next",
            )
            .await
            .unwrap();
        assert_eq!(next.nonce(), first_nonce, "the pair's nonces are released");

        transfer
            .hop
            .persist_lone_approve(&store, &id, &next, HopDirection::FromHub)
            .await
            .unwrap_err();
        let after = wallet
            .prepare_pending(contracts.stable, quote.approve.unwrap().data, "after")
            .await
            .unwrap();
        assert_eq!(
            after.nonce(),
            first_nonce,
            "the lone approve's nonce is released"
        );
    }

    /// A transfer from the hub held at `SwapQuoted` sends again the approves
    /// that went out alone before it stops: a node may have dropped them, and
    /// every later send from the shared Ethereum wallet waits behind their
    /// nonces.
    #[tokio::test]
    async fn hold_at_the_hub_sends_its_split_approves_again() {
        let anvil = spawn_anvil(Anvil::new());
        let (wallet, contracts) = funded_relay_end(&anvil).await;
        let store = Arc::new(test_store(setup_test_db().await, ()));
        let server = MockServer::start();
        let relay_api = MockServer::start();
        mock_quote_refused(&relay_api);
        let transfer = relay_transfer(
            &server,
            &relay_api,
            wallet.clone(),
            wallet.clone(),
            contracts,
            store.clone(),
        )
        .await;
        let id = UsdcRebalanceId(Uuid::new_v4());
        let mut expired = exact_quote(&wallet, contracts, TOWARD_CHAIN_ORDER_ID).await;
        expired.deadline = Utc::now() - chrono::Duration::minutes(1);
        record_quoted_toward_chain(&store, &id, expired.clone()).await;
        let approve = wallet
            .prepare_pending(
                contracts.stable,
                expired.approve.unwrap().data,
                "split approve",
            )
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

        transfer
            .resume_alpaca_to_base(&id, Usdc::new(float!(100)), ROBINHOOD_RELAY)
            .await
            .unwrap();

        let state = store.load(&id).await.unwrap().unwrap();
        assert_eq!(state.state_name(), "SwapQuoted", "{state:?}");
        assert!(
            wallet
                .provider()
                .get_transaction_receipt(approve.tx_hash())
                .await
                .unwrap()
                .is_some(),
            "the split approve is sent before the hold"
        );
    }
}

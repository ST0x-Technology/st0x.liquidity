//! [`RelayBridge`]: the deposit we send through Relay's depository and the
//! proof of the fill or refund Relay's solver sends back.

use std::time::Duration;

use alloy::consensus::Transaction as _;
use alloy::primitives::{Address, B256, Bytes, TxHash, U256};
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, Log, Transaction, TransactionReceipt};
use alloy::sol_types::{SolCall, SolEvent};
use alloy::transports::{RpcError, TransportErrorKind};
use async_trait::async_trait;
use tracing::{debug, info, warn};

use st0x_evm::{Chain, EvmError, IERC20, PreparedTransaction, Wallet};

use super::proof::{
    PaymentTerms, RelayErc20Deposit, UnverifiedReason, check_payment, deposit_event,
};
use super::quote::{StepTerms, StepTransaction, check_steps};
use super::{AcceptedRelayQuote, QuoteField, QuoteMismatch, RelayOrderId};
use crate::{
    DepositScan, HopDirection, PreparedSwap, PreparedSwapDeposit, SwapBridge, SwapDeposit,
    SwapPayment, SwapSide,
};

/// The deposit's gas limit before the wallet's pad: the larger `gasUsed` of
/// the funded test's two deposits (57,114 on Robinhood, 49,083 on Ethereum).
/// Pinned because the deposit cannot be estimated while its approve is
/// unmined.
///
/// Robinhood is an Arbitrum Orbit chain. Its receipt shows `gasUsedForL1` of 0
/// today; if it turns on L1 pricing, gas units include an L1 part and the pin
/// may need to cover it.
const RELAY_DEPOSIT_GAS_LIMIT: u64 = 57_114;

/// Blocks per `eth_getLogs` call of a deposit scan.
const DEPOSIT_LOG_CHUNK: u64 = 10_000;

/// Reads of each chunk of a deposit scan: one lagging load-balanced node can
/// answer a range empty, as `CctpEndpoint::find_recent_burn` also guards.
const SCAN_ATTEMPTS: u32 = 5;

/// Pause between reads of a chunk, so another node may answer.
const SCAN_RETRY_BACKOFF: Duration = Duration::from_millis(150);

/// Who builds a [`RelayBridge`]: the corridor chain and a wallet on each end,
/// with the confirmations each end requires of a deposit, a payment and a
/// deposit scan.
pub struct RelayCtx<EthWallet, ChainWallet> {
    /// The corridor chain Relay connects to the Ethereum hub.
    pub chain: Chain,
    pub ethereum_wallet: EthWallet,
    pub chain_wallet: ChainWallet,
    pub ethereum_confirmations: u64,
    pub chain_confirmations: u64,
}

/// Locally deployed stand-ins for one end's stable and depository.
#[cfg(any(test, feature = "mock"))]
#[derive(Debug, Clone, Copy)]
pub struct RelayEndContracts {
    pub stable: Address,
    pub depository: Address,
}

/// Relay between the Ethereum hub and one corridor chain, both ways.
pub struct RelayBridge<EthWallet, ChainWallet> {
    hub: RelayEnd<EthWallet>,
    chain: RelayEnd<ChainWallet>,
    scan_pace: ScanPace,
}

/// How a deposit scan reads the logs.
#[derive(Debug, Clone, Copy)]
struct ScanPace {
    /// Blocks per `eth_getLogs` call.
    chunk: u64,
    backoff: Duration,
}

/// One chain's stable, depository and our wallet there.
struct RelayEnd<W> {
    chain: Chain,
    stable: Address,
    depository: Address,
    wallet: W,
    /// A deposit, a payment and a deposit scan count only blocks with this
    /// many confirmations, counting the inclusion block as the wallet does.
    confirmations: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum RelayBridgeError {
    #[error("Relay has no depository pinned on {chain}")]
    NoDepository { chain: Chain },
    #[error("the corridor chain must not be the Ethereum hub")]
    HubAsCorridorChain,
    #[error("hub wallet {hub} and corridor chain wallet {chain} differ")]
    WalletsDiffer { hub: Address, chain: Address },
    #[error("{chain} needs at least one confirmation, or a deposit scan reads the raw head")]
    ZeroConfirmations { chain: Chain },
    /// A step does not match the quote or the end it is signed on.
    #[error(transparent)]
    Quote(#[from] QuoteMismatch),
    #[error("quote moves {origin} -> {destination}, not {direction:?}")]
    DirectionMismatch {
        direction: HopDirection,
        origin: Chain,
        destination: Chain,
    },
    #[error("prepared approve has nonce {approve}, the deposit {deposit}: not consecutive")]
    PairNonces { approve: u64, deposit: u64 },
    #[error("tx {tx} has {confirmations} confirmations, the bridge needs {required}")]
    Unconfirmed {
        tx: TxHash,
        confirmations: u64,
        required: u64,
    },
    #[error("tx {tx} is not from the canonical block at height {block}")]
    NotCanonical { tx: TxHash, block: u64 },
    #[error("deposit {tx} reverted")]
    DepositReverted { tx: TxHash },
    #[error("approve {tx} reverted")]
    ApproveReverted { tx: TxHash },
    #[error("deposit {tx} emitted no deposit for order {order_id} from our wallet")]
    DepositUnverified { tx: TxHash, order_id: RelayOrderId },
    #[error("deposit log for order {order_id} carries no tx hash, block or block hash")]
    DepositLogIncomplete { order_id: RelayOrderId },
    #[error("deposit log names tx {tx}, whose receipt the node does not have yet")]
    DepositReceiptMissing { tx: TxHash },
    #[error("tx {tx} is on neither chain yet")]
    TxNotFound { tx: TxHash },
    #[error("fill {txs:?} is unverified: {reason:?}")]
    FillUnverified {
        txs: Vec<TxHash>,
        reason: UnverifiedReason,
    },
    #[error("refund {txs:?} is unverified: {reason:?}")]
    RefundUnverified {
        txs: Vec<TxHash>,
        reason: UnverifiedReason,
    },
    #[error(transparent)]
    Evm(#[from] EvmError),
    #[error("RPC error: {0}")]
    Rpc(#[from] RpcError<TransportErrorKind>),
}

impl<EthWallet: Wallet, ChainWallet: Wallet> RelayBridge<EthWallet, ChainWallet> {
    pub fn try_from_ctx(ctx: RelayCtx<EthWallet, ChainWallet>) -> Result<Self, RelayBridgeError> {
        if ctx.chain == Chain::Ethereum {
            return Err(RelayBridgeError::HubAsCorridorChain);
        }

        // A quote names one refund address and a refund is proven against
        // our wallet on the side it lands on, so a split address would have
        // every quote refused at signing, after its transfer has started.
        let (hub, chain) = (ctx.ethereum_wallet.address(), ctx.chain_wallet.address());
        if hub != chain {
            return Err(RelayBridgeError::WalletsDiffer { hub, chain });
        }

        Ok(Self {
            hub: RelayEnd::pinned(
                Chain::Ethereum,
                ctx.ethereum_wallet,
                ctx.ethereum_confirmations,
            )?,
            chain: RelayEnd::pinned(ctx.chain, ctx.chain_wallet, ctx.chain_confirmations)?,
            scan_pace: ScanPace {
                chunk: DEPOSIT_LOG_CHUNK,
                backoff: SCAN_RETRY_BACKOFF,
            },
        })
    }

    /// Points each end at locally deployed contracts.
    #[cfg(any(test, feature = "mock"))]
    #[must_use]
    pub fn with_local_contracts(
        mut self,
        hub: RelayEndContracts,
        chain: RelayEndContracts,
    ) -> Self {
        self.hub.stable = hub.stable;
        self.hub.depository = hub.depository;
        self.chain.stable = chain.stable;
        self.chain.depository = chain.depository;
        self
    }

    /// Scans in chunks of `blocks`, so a test covers the chunk boundaries.
    #[cfg(test)]
    #[must_use]
    fn with_log_chunk(mut self, blocks: u64) -> Self {
        self.scan_pace.chunk = blocks;
        self
    }

    /// Rereads a scan's chunks without pausing, so tests stay fast.
    #[cfg(test)]
    #[must_use]
    fn without_scan_backoff(mut self) -> Self {
        self.scan_pace.backoff = Duration::ZERO;
        self
    }

    /// The quote must move `direction`'s way, pay the fill to our wallet on
    /// the destination and every refund to our wallet on both ends: a payment
    /// is proven against the wallet of the side it lands on, so a quote
    /// paying anyone else would be signed and never proven.
    fn check_ends(
        &self,
        direction: HopDirection,
        quote: &AcceptedRelayQuote,
    ) -> Result<(), RelayBridgeError> {
        let (origin, destination) = match direction {
            HopDirection::ToHub => (self.chain.chain, self.hub.chain),
            HopDirection::FromHub => (self.hub.chain, self.chain.chain),
        };

        if (quote.origin(), quote.destination()) != (origin, destination) {
            return Err(RelayBridgeError::DirectionMismatch {
                direction,
                origin: quote.origin(),
                destination: quote.destination(),
            });
        }

        let destination_wallet = match direction {
            HopDirection::ToHub => self.hub.wallet.address(),
            HopDirection::FromHub => self.chain.wallet.address(),
        };

        check_wallet(QuoteField::Recipient, destination_wallet, quote.recipient())?;

        for wallet in [self.hub.wallet.address(), self.chain.wallet.address()] {
            check_wallet(QuoteField::RefundRecipient, wallet, quote.refund_to())?;
        }

        Ok(())
    }

    /// Finds `tx` on the origin or the destination chain of `direction`.
    async fn locate(
        &self,
        direction: HopDirection,
        tx: TxHash,
    ) -> Result<Option<(SwapSide, Transaction)>, ProofError> {
        let (on_hub, on_chain) = (
            self.hub
                .wallet
                .provider()
                .get_transaction_by_hash(tx)
                .await?,
            self.chain
                .wallet
                .provider()
                .get_transaction_by_hash(tx)
                .await?,
        );

        let (on_origin, on_destination) = match direction {
            HopDirection::ToHub => (on_chain, on_hub),
            HopDirection::FromHub => (on_hub, on_chain),
        };

        match (on_origin, on_destination) {
            (None, None) => Ok(None),
            (Some(found), None) => Ok(Some((SwapSide::Origin, found))),
            (None, Some(found)) => Ok(Some((SwapSide::Destination, found))),
            (Some(_), Some(_)) => Err(ProofError::Unverified(UnverifiedReason::OnBothChains)),
        }
    }

    /// Proves `found` as a payment for `order_id` on `side` and returns the
    /// amount it paid.
    async fn prove(
        &self,
        direction: HopDirection,
        side: SwapSide,
        tx: TxHash,
        found: &Transaction,
        order_id: RelayOrderId,
    ) -> Result<U256, ProofError> {
        match (direction, side) {
            (HopDirection::ToHub, SwapSide::Origin)
            | (HopDirection::FromHub, SwapSide::Destination) => {
                self.chain.prove(tx, found, order_id).await
            }
            (HopDirection::ToHub, SwapSide::Destination)
            | (HopDirection::FromHub, SwapSide::Origin) => {
                self.hub.prove(tx, found, order_id).await
            }
        }
    }

    /// Locates the one tx in `txs`, with the side it is on.
    async fn locate_one(
        &self,
        direction: HopDirection,
        txs: &[TxHash],
    ) -> Result<Located, ProofError> {
        let [tx] = txs else {
            return Err(ProofError::Unverified(UnverifiedReason::TxCount {
                count: txs.len(),
            }));
        };

        let (side, found) = self
            .locate(direction, *tx)
            .await?
            .ok_or(ProofError::NotFound { tx: *tx })?;

        Ok(Located {
            tx: *tx,
            side,
            found,
        })
    }

    /// Proves the located `tx` paid `order_id` on `side`.
    async fn verify_payment(
        &self,
        direction: HopDirection,
        order_id: RelayOrderId,
        Located { tx, side, found }: Located,
    ) -> Result<SwapPayment, ProofError> {
        let amount = self.prove(direction, side, tx, &found, order_id).await?;

        Ok(SwapPayment { tx, side, amount })
    }
}

#[async_trait]
impl<EthWallet: Wallet, ChainWallet: Wallet> SwapBridge for RelayBridge<EthWallet, ChainWallet> {
    type Error = RelayBridgeError;
    type Quote = AcceptedRelayQuote;
    type OrderId = RelayOrderId;

    async fn prepare_deposit(
        &self,
        direction: HopDirection,
        quote: &AcceptedRelayQuote,
    ) -> Result<PreparedSwap, RelayBridgeError> {
        self.check_ends(direction, quote)?;

        match direction {
            HopDirection::ToHub => self.chain.prepare_deposit(quote).await,
            HopDirection::FromHub => self.hub.prepare_deposit(quote).await,
        }
    }

    async fn discard_prepared(&self, direction: HopDirection, prepared: &PreparedSwap) {
        match direction {
            HopDirection::ToHub => self.chain.discard_prepared(prepared).await,
            HopDirection::FromHub => self.hub.discard_prepared(prepared).await,
        }
    }

    async fn restore_prepared(&self, direction: HopDirection, prepared: &PreparedSwap) {
        match direction {
            HopDirection::ToHub => self.chain.restore_prepared(prepared).await,
            HopDirection::FromHub => self.hub.restore_prepared(prepared).await,
        }
    }

    async fn broadcast_deposit(
        &self,
        direction: HopDirection,
        prepared: &PreparedSwapDeposit,
    ) -> Result<TxHash, RelayBridgeError> {
        match direction {
            HopDirection::ToHub => self.chain.broadcast_deposit(prepared).await,
            HopDirection::FromHub => self.hub.broadcast_deposit(prepared).await,
        }
    }

    async fn broadcast_approve(
        &self,
        direction: HopDirection,
        approve: &PreparedTransaction,
    ) -> Result<TxHash, RelayBridgeError> {
        match direction {
            HopDirection::ToHub => self.chain.broadcast_approve(approve).await,
            HopDirection::FromHub => self.hub.broadcast_approve(approve).await,
        }
    }

    async fn confirm_deposit(
        &self,
        direction: HopDirection,
        order_id: RelayOrderId,
        prepared: &PreparedSwapDeposit,
        split_approves: &[PreparedTransaction],
    ) -> Result<SwapDeposit<RelayOrderId>, RelayBridgeError> {
        match direction {
            HopDirection::ToHub => {
                self.chain
                    .confirm_deposit(order_id, prepared, split_approves)
                    .await
            }
            HopDirection::FromHub => {
                self.hub
                    .confirm_deposit(order_id, prepared, split_approves)
                    .await
            }
        }
    }

    async fn confirm_approve(
        &self,
        direction: HopDirection,
        approve: &PreparedTransaction,
    ) -> Result<(), RelayBridgeError> {
        match direction {
            HopDirection::ToHub => self.chain.confirm_approve(approve).await,
            HopDirection::FromHub => self.hub.confirm_approve(approve).await,
        }
    }

    async fn origin_block(&self, direction: HopDirection) -> Result<u64, RelayBridgeError> {
        let head = match direction {
            HopDirection::ToHub => self.chain.wallet.provider().get_block_number().await?,
            HopDirection::FromHub => self.hub.wallet.provider().get_block_number().await?,
        };

        Ok(head)
    }

    async fn find_recent_deposits(
        &self,
        direction: HopDirection,
        order_ids: &[RelayOrderId],
        from_block: u64,
    ) -> Result<DepositScan<RelayOrderId>, RelayBridgeError> {
        match direction {
            HopDirection::ToHub => {
                self.chain
                    .find_deposits(order_ids, from_block, self.scan_pace)
                    .await
            }
            HopDirection::FromHub => {
                self.hub
                    .find_deposits(order_ids, from_block, self.scan_pace)
                    .await
            }
        }
    }

    async fn verify_fill(
        &self,
        direction: HopDirection,
        order_id: RelayOrderId,
        minimum_out: U256,
        txs: &[TxHash],
    ) -> Result<SwapPayment, RelayBridgeError> {
        let unverified = |reason| RelayBridgeError::FillUnverified {
            txs: txs.to_vec(),
            reason,
        };

        let located = self
            .locate_one(direction, txs)
            .await
            .map_err(|error| error.into_bridge_error(unverified))?;

        // Only a refund pays on the origin chain: refused before the proof,
        // which would wait for its confirmations first.
        if located.side == SwapSide::Origin {
            return Err(unverified(UnverifiedReason::OnOriginChain));
        }

        let payment = self
            .verify_payment(direction, order_id, located)
            .await
            .map_err(|error| error.into_bridge_error(unverified))?;

        if payment.amount < minimum_out {
            return Err(unverified(UnverifiedReason::BelowMinimum {
                amount: payment.amount,
                minimum: minimum_out,
            }));
        }

        info!(target: "bridge", %order_id, ?payment, "Relay fill verified");
        Ok(payment)
    }

    /// Both stables have 6 decimals (pinned by a test on the quote checks), so
    /// a refund in either compares with `deposited` unit for unit.
    async fn verify_refund(
        &self,
        direction: HopDirection,
        order_id: RelayOrderId,
        deposited: U256,
        txs: &[TxHash],
    ) -> Result<SwapPayment, RelayBridgeError> {
        let unverified = |reason| RelayBridgeError::RefundUnverified {
            txs: txs.to_vec(),
            reason,
        };

        let located = self
            .locate_one(direction, txs)
            .await
            .map_err(|error| error.into_bridge_error(unverified))?;
        let payment = self
            .verify_payment(direction, order_id, located)
            .await
            .map_err(|error| error.into_bridge_error(unverified))?;

        if payment.amount > deposited {
            return Err(unverified(UnverifiedReason::AboveDeposit {
                amount: payment.amount,
                deposited,
            }));
        }

        info!(target: "bridge", %order_id, ?payment, "Relay refund verified");
        Ok(payment)
    }
}

impl<W: Wallet> RelayEnd<W> {
    fn pinned(chain: Chain, wallet: W, confirmations: u64) -> Result<Self, RelayBridgeError> {
        if confirmations == 0 {
            return Err(RelayBridgeError::ZeroConfirmations { chain });
        }

        let depository = chain
            .relay_depository()
            .ok_or(RelayBridgeError::NoDepository { chain })?;

        Ok(Self {
            chain,
            stable: chain.settlement_stable().address,
            depository,
            wallet,
            confirmations,
        })
    }

    /// Signs the approve (the quote's, or our own exact one when the quote has
    /// none) only when the allowance falls short, and the deposit, at the next
    /// nonces.
    /// Another send from this wallet between the two signs takes the nonce in
    /// between. Then only the deposit is discarded: the approve is returned to
    /// go out alone, so its nonce is not left as a gap the other send waits
    /// behind.
    async fn prepare_deposit(
        &self,
        quote: &AcceptedRelayQuote,
    ) -> Result<PreparedSwap, RelayBridgeError> {
        let wallet_chain = self.wallet.provider().get_chain_id().await?;

        self.check_steps(quote, wallet_chain)?;

        let approve = self
            .approve_if_short(quote.amounts().amount_in, quote.approve())
            .await?;

        let approve = match approve {
            Some(calldata) => Some(
                self.wallet
                    .prepare_pending(self.stable, calldata, "Relay deposit approve")
                    .await?,
            ),
            None => None,
        };

        let deposit = self
            .wallet
            .prepare_pending_with_gas_limit(
                self.depository,
                quote.deposit().data.clone(),
                RELAY_DEPOSIT_GAS_LIMIT,
                "Relay deposit",
            )
            .await;

        let deposit = match deposit {
            Ok(deposit) => deposit,
            Err(error) => {
                if let Some(approve) = &approve {
                    warn!(
                        target: "bridge",
                        ?error,
                        approve = %approve.tx_hash(),
                        "Relay deposit signing failed, discarding its approve"
                    );
                    self.wallet.discard_prepared(approve.tx_hash()).await;
                }
                return Err(error.into());
            }
        };

        if let Some(approve) = &approve
            && let Err(error) = check_pair_nonces(approve, &deposit)
        {
            warn!(
                target: "bridge",
                ?error,
                approve = %approve.tx_hash(),
                deposit = %deposit.tx_hash(),
                "Relay deposit pair is not consecutive, discarding the deposit, \
                 the approve goes alone"
            );
            self.wallet.discard_prepared(deposit.tx_hash()).await;
            return Ok(PreparedSwap::ApproveOnly {
                approve: approve.clone(),
            });
        }

        debug!(
            target: "bridge",
            chain = %self.chain,
            order_id = %quote.order_id(),
            approve = ?approve.as_ref().map(PreparedTransaction::tx_hash),
            deposit = %deposit.tx_hash(),
            "Relay deposit signed"
        );

        Ok(PreparedSwap::Deposit(PreparedSwapDeposit {
            approve,
            deposit,
        }))
    }

    /// [`check_steps`] again, against this end: the steps must be for this
    /// wallet's chain, call this end's stable and depository with no native
    /// value, and move the quoted amount from our wallet for the quote's order.
    fn check_steps(
        &self,
        quote: &AcceptedRelayQuote,
        wallet_chain: u64,
    ) -> Result<(), QuoteMismatch> {
        let calldata = check_steps(
            quote.approve(),
            quote.deposit(),
            &StepTerms {
                chain_id: wallet_chain,
                stable: self.stable,
                depository: self.depository,
                depositor: self.wallet.address(),
                amount: quote.amounts().amount_in,
            },
        )?;

        let quoted = quote.order_id();
        if calldata != quoted {
            return Err(QuoteMismatch::OrderIdMismatch { quoted, calldata });
        }

        Ok(())
    }

    /// The approve calldata to sign, `None` when the standing allowance covers
    /// the deposit: an approve sent alone after a split pair stands, and
    /// signing the quote's again would cost gas and a new split window. With
    /// no approve step, another transfer from this wallet may have spent the
    /// allowance, so a short one gets our own exact approve.
    async fn approve_if_short(
        &self,
        amount: U256,
        step: Option<&StepTransaction>,
    ) -> Result<Option<Bytes>, RelayBridgeError> {
        let allowance = IERC20::new(self.stable, self.wallet.provider())
            .allowance(self.wallet.address(), self.depository)
            .call()
            .await
            .map_err(EvmError::from)?;

        if allowance >= amount {
            return Ok(None);
        }

        if let Some(step) = step {
            return Ok(Some(step.data.clone()));
        }

        warn!(
            target: "bridge",
            %allowance,
            %amount,
            "Relay quote has no approve and the allowance is short, approving"
        );

        let spender = self.depository;
        Ok(Some(Bytes::from(
            IERC20::approveCall { spender, amount }.abi_encode(),
        )))
    }

    /// The deposit goes first: discarding the approve while its deposit is
    /// still held rewinds the wallet's nonces under the deposit.
    async fn discard_prepared(&self, prepared: &PreparedSwap) {
        let approve = match prepared {
            PreparedSwap::Deposit(pair) => {
                self.wallet.discard_prepared(pair.deposit.tx_hash()).await;
                pair.approve.as_ref()
            }
            PreparedSwap::ApproveOnly { approve } => Some(approve),
        };

        if let Some(approve) = approve {
            self.wallet.discard_prepared(approve.tx_hash()).await;
        }
    }

    async fn restore_prepared(&self, prepared: &PreparedSwap) {
        let (approve, deposit) = match prepared {
            PreparedSwap::Deposit(pair) => (pair.approve.as_ref(), Some(&pair.deposit)),
            PreparedSwap::ApproveOnly { approve } => (Some(approve), None),
        };

        for prepared in approve.into_iter().chain(deposit) {
            self.wallet.restore_prepared(prepared).await;
        }
    }

    async fn broadcast_deposit(
        &self,
        prepared: &PreparedSwapDeposit,
    ) -> Result<TxHash, RelayBridgeError> {
        if let Some(approve) = &prepared.approve {
            check_pair_nonces(approve, &prepared.deposit)?;

            self.wallet
                .broadcast_prepared(approve, "Relay deposit approve")
                .await?;
        }

        Ok(self
            .wallet
            .broadcast_prepared(&prepared.deposit, "Relay deposit")
            .await?)
    }

    async fn broadcast_approve(
        &self,
        approve: &PreparedTransaction,
    ) -> Result<TxHash, RelayBridgeError> {
        Ok(self
            .wallet
            .broadcast_prepared(approve, "Relay deposit approve, alone")
            .await?)
    }

    async fn confirm_deposit(
        &self,
        order_id: RelayOrderId,
        prepared: &PreparedSwapDeposit,
        split_approves: &[PreparedTransaction],
    ) -> Result<SwapDeposit<RelayOrderId>, RelayBridgeError> {
        let tx = prepared.deposit.tx_hash();

        if let Some(approve) = &prepared.approve {
            check_pair_nonces(approve, &prepared.deposit)?;
        }

        let (receipt, block) = self.confirmed_receipt(tx).await?;

        // The deposit is mined, reverted or not, so every nonce below it is
        // used: nothing else can mine at its approve's or a split approve's.
        let below = prepared
            .approve
            .iter()
            .chain(split_approves)
            .filter(|approve| approve.nonce() < prepared.deposit.nonce());
        for approve in below {
            self.wallet.release_superseded(approve.tx_hash()).await;
        }

        if !receipt.status() {
            return Err(RelayBridgeError::DepositReverted { tx });
        }

        let deposit = receipt
            .inner
            .logs()
            .iter()
            .filter_map(|log| deposit_event(log, self.depository))
            .find(|event| self.is_ours(event) && RelayOrderId(event.id) == order_id)
            .filter(|_| self.sent_by_us(&receipt))
            .ok_or(RelayBridgeError::DepositUnverified { tx, order_id })?;

        Ok(SwapDeposit {
            tx,
            order_id,
            amount: deposit.amount,
            block,
        })
    }

    async fn confirm_approve(&self, approve: &PreparedTransaction) -> Result<(), RelayBridgeError> {
        let tx = approve.tx_hash();
        let (receipt, _) = self.confirmed_receipt(tx).await?;

        if !receipt.status() {
            return Err(RelayBridgeError::ApproveReverted { tx });
        }

        Ok(())
    }

    /// `tx`'s receipt and its block once it has this end's confirmations in a
    /// canonical block. The wallet's wait may count fewer confirmations, and a
    /// lagging node can serve a receipt from a reorged-out block.
    async fn confirmed_receipt(
        &self,
        tx: TxHash,
    ) -> Result<(TransactionReceipt, u64), RelayBridgeError> {
        let receipt = self.wallet.await_receipt(tx).await?;
        let head = self.wallet.provider().get_block_number().await?;

        let (Some(block), Some(block_hash)) = (receipt.block_number, receipt.block_hash) else {
            return Err(RelayBridgeError::Unconfirmed {
                tx,
                confirmations: 0,
                required: self.confirmations,
            });
        };

        let confirmations = head
            .checked_sub(block)
            .map_or(0, |depth| depth.saturating_add(1));

        if confirmations < self.confirmations {
            return Err(RelayBridgeError::Unconfirmed {
                tx,
                confirmations,
                required: self.confirmations,
            });
        }

        self.ensure_canonical(tx, block, block_hash).await?;

        Ok((receipt, block))
    }

    /// Refuses `tx` as seen in `block_hash` unless that is the canonical
    /// block at height `block`: a lagging node can serve data from a block a
    /// reorg removed.
    async fn ensure_canonical(
        &self,
        tx: TxHash,
        block: u64,
        block_hash: B256,
    ) -> Result<(), RelayBridgeError> {
        let canonical = self
            .wallet
            .provider()
            .get_block_by_number(block.into())
            .await?;

        if canonical.is_none_or(|canonical| canonical.header.hash != block_hash) {
            warn!(
                target: "bridge",
                %tx,
                block,
                %block_hash,
                "Relay tx is not from the canonical block at its height"
            );
            return Err(RelayBridgeError::NotCanonical { tx, block });
        }

        Ok(())
    }

    /// The event's `from` is the caller-supplied depositor, so anyone can
    /// name our wallet: [`Self::sent_by_us`] proves who paid.
    fn is_ours(&self, event: &RelayErc20Deposit) -> bool {
        event.from == self.wallet.address() && event.token == self.stable
    }

    /// The depository pulls the stable from the tx's sender, so a deposit is
    /// ours only when our wallet called the depository itself.
    fn sent_by_us(&self, receipt: &TransactionReceipt) -> bool {
        receipt.from == self.wallet.address() && receipt.to == Some(self.depository)
    }

    /// `RelayErc20Deposit` has no indexed field, so every deposit log in the
    /// range is fetched and decoded, chunk by chunk. The scan stops at the
    /// newest block with `confirmations` (counting the inclusion block, as the
    /// wallet does): a lagging load-balanced node may not have indexed the
    /// newest blocks, and a deposit there is unconfirmed.
    async fn find_deposits(
        &self,
        order_ids: &[RelayOrderId],
        from_block: u64,
        pace: ScanPace,
    ) -> Result<DepositScan<RelayOrderId>, RelayBridgeError> {
        let head = self.wallet.provider().get_block_number().await?;

        let confirmed = head.saturating_sub(self.confirmations.saturating_sub(1));
        let scanned_to = confirmed.max(from_block.saturating_sub(1));
        let mut deposits = Vec::new();
        let mut start = from_block;

        while start <= scanned_to {
            let end = start
                .saturating_add(pace.chunk.saturating_sub(1))
                .min(scanned_to);

            deposits.extend(self.read_chunk(order_ids, start, end, pace).await?);

            start = end.saturating_add(1);
        }

        debug!(
            target: "bridge",
            chain = %self.chain,
            from_block,
            head,
            scanned_to,
            found = deposits.len(),
            "Relay deposit scan done"
        );

        Ok(DepositScan {
            deposits,
            scanned_to,
        })
    }

    /// Our deposits for `order_ids` in blocks `start..=end`. A load-balanced
    /// node may lag and answer a range empty, so the chunk is read
    /// `SCAN_ATTEMPTS` times and a deposit any read returns is kept. A log
    /// from a reorged-out block in any read fails the chunk, to be scanned
    /// again.
    async fn read_chunk(
        &self,
        order_ids: &[RelayOrderId],
        start: u64,
        end: u64,
        pace: ScanPace,
    ) -> Result<Vec<SwapDeposit<RelayOrderId>>, RelayBridgeError> {
        let filter = Filter::new()
            .address(self.depository)
            .event_signature(RelayErc20Deposit::SIGNATURE_HASH)
            .from_block(start)
            .to_block(end);

        let mut candidates = Vec::new();

        for attempt in 1..=SCAN_ATTEMPTS {
            let logs = self.wallet.provider().get_logs(&filter).await?;

            for log in &logs {
                if let Some(candidate) = self.candidate(log, order_ids)?
                    && !candidates.contains(&candidate)
                {
                    candidates.push(candidate);
                }
            }

            if attempt < SCAN_ATTEMPTS {
                tokio::time::sleep(pace.backoff).await;
            }
        }

        let mut deposits = Vec::with_capacity(candidates.len());

        for (deposit, block_hash) in candidates {
            let receipt = self
                .wallet
                .provider()
                .get_transaction_receipt(deposit.tx)
                .await?
                .ok_or(RelayBridgeError::DepositReceiptMissing { tx: deposit.tx })?;

            if !self.sent_by_us(&receipt) {
                warn!(
                    target: "bridge",
                    tx = %deposit.tx,
                    order_id = %deposit.order_id,
                    from = %receipt.from,
                    "Relay deposit names our wallet but another account sent it"
                );
                continue;
            }

            self.ensure_canonical(deposit.tx, deposit.block, block_hash)
                .await?;

            deposits.push(deposit);
        }

        deposits.sort_by_key(|deposit| deposit.block);
        Ok(deposits)
    }

    /// The deposit `log` records and the hash of its block, when it names our
    /// wallet and stable and one of `order_ids`; who sent it and whether its
    /// block is canonical are checked after.
    fn candidate(
        &self,
        log: &Log,
        order_ids: &[RelayOrderId],
    ) -> Result<Option<(SwapDeposit<RelayOrderId>, B256)>, RelayBridgeError> {
        let Some(event) = deposit_event(log, self.depository) else {
            return Ok(None);
        };
        let order_id = RelayOrderId(event.id);

        if !self.is_ours(&event) || !order_ids.contains(&order_id) {
            return Ok(None);
        }

        let (Some(tx), Some(block), Some(block_hash)) =
            (log.transaction_hash, log.block_number, log.block_hash)
        else {
            return Err(RelayBridgeError::DepositLogIncomplete { order_id });
        };

        let deposit = SwapDeposit {
            tx,
            order_id,
            amount: event.amount,
            block,
        };

        Ok(Some((deposit, block_hash)))
    }

    /// Requires `found` to have this chain's confirmations, then checks it
    /// paid our wallet this end's stable for `order_id`.
    async fn prove(
        &self,
        tx: TxHash,
        found: &Transaction,
        order_id: RelayOrderId,
    ) -> Result<U256, ProofError> {
        let (receipt, _) = self.confirmed_receipt(tx).await?;

        let terms = PaymentTerms {
            stable: self.stable,
            recipient: self.wallet.address(),
            order_id,
        };

        check_payment(&terms, found.to(), found.input(), &receipt).map_err(ProofError::Unverified)
    }
}

fn check_wallet(field: QuoteField, wallet: Address, quoted: Address) -> Result<(), QuoteMismatch> {
    if quoted == wallet {
        return Ok(());
    }

    Err(QuoteMismatch::AddressMismatch {
        field,
        expected: wallet,
        actual: quoted,
    })
}

/// The approve must sit at the nonce right before its deposit.
fn check_pair_nonces(
    approve: &PreparedTransaction,
    deposit: &PreparedTransaction,
) -> Result<(), RelayBridgeError> {
    if approve.nonce().checked_add(1) == Some(deposit.nonce()) {
        return Ok(());
    }

    Err(RelayBridgeError::PairNonces {
        approve: approve.nonce(),
        deposit: deposit.nonce(),
    })
}

/// The tx Relay named, found on one side of the swap.
struct Located {
    tx: TxHash,
    side: SwapSide,
    found: Transaction,
}

/// A proof that failed: a tx that is not the payment, or a read that did not
/// finish.
enum ProofError {
    Unverified(UnverifiedReason),
    NotFound {
        tx: TxHash,
    },
    /// A failure that is not about the proof itself, passed through as is.
    Bridge(RelayBridgeError),
    Evm(EvmError),
    Rpc(RpcError<TransportErrorKind>),
}

impl From<EvmError> for ProofError {
    fn from(error: EvmError) -> Self {
        Self::Evm(error)
    }
}

impl From<RelayBridgeError> for ProofError {
    fn from(error: RelayBridgeError) -> Self {
        Self::Bridge(error)
    }
}

impl From<RpcError<TransportErrorKind>> for ProofError {
    fn from(error: RpcError<TransportErrorKind>) -> Self {
        Self::Rpc(error)
    }
}

impl ProofError {
    fn into_bridge_error(
        self,
        unverified: impl FnOnce(UnverifiedReason) -> RelayBridgeError,
    ) -> RelayBridgeError {
        match self {
            Self::Unverified(reason) => unverified(reason),
            Self::NotFound { tx } => RelayBridgeError::TxNotFound { tx },
            Self::Bridge(error) => error,
            Self::Evm(error) => RelayBridgeError::Evm(error),
            Self::Rpc(error) => RelayBridgeError::Rpc(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::SystemTime;

    use alloy::consensus::TxEnvelope;
    use alloy::eips::Decodable2718;
    use alloy::primitives::Signature;
    use alloy::providers::{DynProvider, ProviderBuilder, RootProvider};
    use alloy::transports::TransportResult;

    use st0x_evm::Evm;
    use st0x_evm::local::RawPrivateKeyWallet;

    use super::super::quote::depositErc20Call;
    use super::super::test_chain::RelayChain;
    use super::super::{
        BasisPoints, QuoteAmounts, QuoteBounds, QuoteFees, QuoteStep, RelayQuote, RelayRequestId,
        StepTransaction,
    };
    use super::*;

    type TestWallet = RawPrivateKeyWallet<DynProvider>;

    const AMOUNT: U256 = U256::from_limbs([5_000_000, 0, 0, 0]);

    const MINIMUM_OUT: U256 = U256::from_limbs([4_749_464, 0, 0, 0]);

    /// The confirmations a deposit scan requires on either end.
    const CONFIRMATIONS: u64 = 3;

    /// Our wallet's view of the hub and a corridor chain, each an Anvil chain
    /// with a stable and a depository.
    struct Harness {
        hub: RelayChain,
        chain: RelayChain,
        bridge: RelayBridge<TestWallet, TestWallet>,
    }

    impl Harness {
        async fn new() -> Self {
            let hub = RelayChain::spawn(1, 31_337).await;
            let chain = RelayChain::spawn(4, 31_338).await;

            let bridge = RelayBridge::try_from_ctx(RelayCtx {
                chain: Chain::Robinhood,
                ethereum_wallet: wallet(&hub),
                chain_wallet: wallet(&chain),
                ethereum_confirmations: CONFIRMATIONS,
                chain_confirmations: CONFIRMATIONS,
            })
            .unwrap()
            .with_local_contracts(contracts(&hub), contracts(&chain))
            .without_scan_backoff();

            Self { hub, chain, bridge }
        }

        /// Signs and broadcasts a Robinhood -> hub deposit pair, mined in one
        /// block.
        async fn send_deposit_to_hub(&self, order_id: B256) -> PreparedSwapDeposit {
            self.chain.mint(self.chain.wallet(), AMOUNT).await;
            let quote = accepted(quote(&self.chain, order_id, true));

            let prepared = self
                .bridge
                .prepare_deposit(HopDirection::ToHub, &quote)
                .await
                .map(deposit_pair)
                .unwrap();

            self.bridge
                .broadcast_deposit(HopDirection::ToHub, &prepared)
                .await
                .unwrap();

            prepared
        }

        /// [`Self::send_deposit_to_hub`], then mines to its confirmations and
        /// confirms it.
        async fn deposit_to_hub(&self, order_id: B256) -> SwapDeposit<RelayOrderId> {
            let pair = self.send_deposit_to_hub(order_id).await;
            self.chain.mine(CONFIRMATIONS - 1).await;

            self.bridge
                .confirm_deposit(HopDirection::ToHub, RelayOrderId(order_id), &pair, &[])
                .await
                .unwrap()
        }
    }

    fn wallet(chain: &RelayChain) -> TestWallet {
        let provider = ProviderBuilder::new()
            .connect_http(chain.endpoint().parse().unwrap())
            .erased();

        RawPrivateKeyWallet::new(&chain.wallet_key(), provider, 1).unwrap()
    }

    /// A load-balanced node that answers its first `empty_answers` log queries
    /// empty, as a replica behind the head does.
    struct LaggingLogs {
        inner: DynProvider,
        empty_answers: AtomicU32,
    }

    #[async_trait]
    impl Provider for LaggingLogs {
        fn root(&self) -> &RootProvider {
            self.inner.root()
        }

        async fn get_logs(&self, filter: &Filter) -> TransportResult<Vec<Log>> {
            let lagging = self
                .empty_answers
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                    left.checked_sub(1)
                })
                .is_ok();

            if lagging {
                return Ok(vec![]);
            }

            self.inner.get_logs(filter).await
        }
    }

    /// Our wallet on `chain`, behind a node whose first `empty_answers` log
    /// queries come back empty.
    fn lagging_wallet(chain: &RelayChain, empty_answers: u32) -> TestWallet {
        let inner = ProviderBuilder::new()
            .connect_http(chain.endpoint().parse().unwrap())
            .erased();
        let provider = LaggingLogs {
            inner,
            empty_answers: AtomicU32::new(empty_answers),
        }
        .erased();

        RawPrivateKeyWallet::new(&chain.wallet_key(), provider, 1).unwrap()
    }

    /// A lagging node that still serves logs from blocks a reorg removed.
    struct StaleLogs {
        inner: DynProvider,
    }

    #[async_trait]
    impl Provider for StaleLogs {
        fn root(&self) -> &RootProvider {
            self.inner.root()
        }

        async fn get_logs(&self, filter: &Filter) -> TransportResult<Vec<Log>> {
            let mut logs = self.inner.get_logs(filter).await?;

            for log in &mut logs {
                log.block_hash = Some(B256::repeat_byte(0xee));
            }

            Ok(logs)
        }
    }

    /// Our wallet on `chain`, behind a node whose logs name reorged-out blocks.
    fn stale_logs_wallet(chain: &RelayChain) -> TestWallet {
        let inner = ProviderBuilder::new()
            .connect_http(chain.endpoint().parse().unwrap())
            .erased();
        let provider = StaleLogs { inner }.erased();

        RawPrivateKeyWallet::new(&chain.wallet_key(), provider, 1).unwrap()
    }

    /// A bridge that scans the corridor chain through `chain_wallet`.
    fn scan_bridge(
        harness: &Harness,
        chain_wallet: TestWallet,
    ) -> RelayBridge<TestWallet, TestWallet> {
        RelayBridge::try_from_ctx(RelayCtx {
            chain: Chain::Robinhood,
            ethereum_wallet: wallet(&harness.hub),
            chain_wallet,
            ethereum_confirmations: CONFIRMATIONS,
            chain_confirmations: CONFIRMATIONS,
        })
        .unwrap()
        .with_local_contracts(contracts(&harness.hub), contracts(&harness.chain))
        .without_scan_backoff()
    }

    /// A wallet on an endpoint nothing listens on, for checks that never
    /// reach the chain.
    fn offline_wallet() -> TestWallet {
        let provider = ProviderBuilder::new()
            .connect_http("http://127.0.0.1:1".parse().unwrap())
            .erased();

        RawPrivateKeyWallet::new(&B256::repeat_byte(0x01), provider, 1).unwrap()
    }

    fn contracts(chain: &RelayChain) -> RelayEndContracts {
        RelayEndContracts {
            stable: chain.stable,
            depository: chain.depository,
        }
    }

    /// A Robinhood -> hub quote for depositing `AMOUNT` on `origin`, shaped
    /// like a checked live one: the fill and refunds pay our wallet, whose
    /// address is the same on both ends.
    fn quote(origin: &RelayChain, order_id: B256, with_approve: bool) -> RelayQuote {
        let step = |to, data: Vec<u8>| StepTransaction {
            chain_id: origin.chain_id(),
            to,
            data: Bytes::from(data),
            value: U256::ZERO,
        };

        RelayQuote {
            request_id: RelayRequestId(B256::repeat_byte(0x11)),
            order_id: RelayOrderId(order_id),
            amounts: QuoteAmounts {
                amount_in: AMOUNT,
                expected_out: U256::from(4_763_755),
                minimum_out: MINIMUM_OUT,
                slippage: BasisPoints::new(30).unwrap(),
            },
            deadline: SystemTime::now() + Duration::from_secs(7 * 24 * 60 * 60),
            fees: QuoteFees {
                relayer: U256::from(236_245),
                gas: U256::from(1_000),
            },
            origin: Chain::Robinhood,
            destination: Chain::Ethereum,
            recipient: origin.wallet(),
            refund_to: origin.wallet(),
            approve: with_approve.then(|| {
                step(
                    origin.stable,
                    IERC20::approveCall {
                        spender: origin.depository,
                        amount: AMOUNT,
                    }
                    .abi_encode(),
                )
            }),
            deposit: step(
                origin.depository,
                depositErc20Call {
                    depositor: origin.wallet(),
                    token: origin.stable,
                    amount: AMOUNT,
                    id: order_id,
                }
                .abi_encode(),
            ),
        }
    }

    /// `quote` accepted within bounds loose enough for its 4.7% relayer fee
    /// plus slippage.
    fn accepted(quote: RelayQuote) -> AcceptedRelayQuote {
        quote
            .accept(&QuoteBounds {
                max_loss: BasisPoints::new(600).unwrap(),
                downstream_minimum: U256::ZERO,
            })
            .unwrap()
    }

    /// Our corridor-chain wallet, with a hook on its first deposit signing, and
    /// with `orphan_receipts` serving every receipt as a lagging node would
    /// from a reorged-out block.
    struct PairWallet {
        inner: TestWallet,
        on_deposit: Mutex<Option<OnDepositSigning>>,
        stable: Address,
        orphan_receipts: bool,
    }

    enum OnDepositSigning {
        /// Another send from this wallet takes the next nonce first.
        AnotherSendFirst,
        Fail,
    }

    #[async_trait]
    impl Evm for PairWallet {
        type Provider = DynProvider;

        fn provider(&self) -> &DynProvider {
            self.inner.provider()
        }
    }

    #[async_trait]
    impl Wallet for PairWallet {
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
            let hook = self.on_deposit.lock().unwrap().take();
            match hook {
                Some(OnDepositSigning::AnotherSendFirst) => {
                    self.inner
                        .prepare_pending(self.stable, probe_calldata(), "another send")
                        .await?;
                }
                Some(OnDepositSigning::Fail) => {
                    return Err(EvmError::Reverted {
                        tx_hash: TxHash::ZERO,
                    });
                }
                None => {}
            }

            self.inner
                .prepare_pending_with_gas_limit(contract, calldata, unpadded_gas_limit, note)
                .await
        }

        async fn broadcast_prepared(
            &self,
            prepared: &PreparedTransaction,
            note: &str,
        ) -> Result<TxHash, EvmError> {
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
            let mut receipt = self.inner.await_receipt(tx_hash).await?;

            if self.orphan_receipts {
                receipt.block_hash = Some(B256::repeat_byte(0xee));
            }

            Ok(receipt)
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

    /// A bridge whose corridor-chain wallet runs `on_deposit` when it first
    /// signs a deposit.
    fn pair_bridge(
        harness: &Harness,
        on_deposit: OnDepositSigning,
    ) -> RelayBridge<TestWallet, PairWallet> {
        hooked_bridge(harness, Some(on_deposit), false)
    }

    fn hooked_bridge(
        harness: &Harness,
        on_deposit: Option<OnDepositSigning>,
        orphan_receipts: bool,
    ) -> RelayBridge<TestWallet, PairWallet> {
        RelayBridge::try_from_ctx(RelayCtx {
            chain: Chain::Robinhood,
            ethereum_wallet: wallet(&harness.hub),
            chain_wallet: PairWallet {
                inner: wallet(&harness.chain),
                on_deposit: Mutex::new(on_deposit),
                stable: harness.chain.stable,
                orphan_receipts,
            },
            ethereum_confirmations: CONFIRMATIONS,
            chain_confirmations: CONFIRMATIONS,
        })
        .unwrap()
        .with_local_contracts(contracts(&harness.hub), contracts(&harness.chain))
    }

    fn probe_calldata() -> Bytes {
        Bytes::from(
            IERC20::approveCall {
                spender: Address::ZERO,
                amount: U256::ZERO,
            }
            .abi_encode(),
        )
    }

    /// The nonces the wallet hands the next two sends it signs.
    async fn next_two_nonces(wallet: &impl Wallet) -> [u64; 2] {
        let mut nonces = [0; 2];

        for nonce in &mut nonces {
            *nonce = wallet
                .prepare_pending(Address::ZERO, probe_calldata(), "nonce probe")
                .await
                .unwrap()
                .nonce();
        }

        nonces
    }

    /// `tx`, sent on the corridor chain by any account, as a pair with no
    /// approve: what a caller handing the wrong tx to `confirm_deposit` holds.
    async fn sent_pair(harness: &Harness, tx: TxHash) -> PreparedSwapDeposit {
        let raw = harness
            .bridge
            .chain
            .wallet
            .provider()
            .get_raw_transaction_by_hash(tx)
            .await
            .unwrap()
            .unwrap();

        PreparedSwapDeposit {
            approve: None,
            deposit: PreparedTransaction::from_raw(raw).unwrap(),
        }
    }

    fn deposit_pair(prepared: PreparedSwap) -> PreparedSwapDeposit {
        match prepared {
            PreparedSwap::Deposit(pair) => pair,
            PreparedSwap::ApproveOnly { approve } => {
                panic!("expected a deposit pair, got approve {approve:?} alone")
            }
        }
    }

    fn gas_limit(prepared: &PreparedTransaction) -> u64 {
        alloy::consensus::Transaction::gas_limit(
            &TxEnvelope::decode_2718_exact(prepared.raw().as_ref()).unwrap(),
        )
    }

    fn unverified_fill(error: RelayBridgeError) -> (Vec<TxHash>, UnverifiedReason) {
        match error {
            RelayBridgeError::FillUnverified { txs, reason } => (txs, reason),
            other => panic!("expected FillUnverified, got {other:?}"),
        }
    }

    fn unverified_refund(error: RelayBridgeError) -> (Vec<TxHash>, UnverifiedReason) {
        match error {
            RelayBridgeError::RefundUnverified { txs, reason } => (txs, reason),
            other => panic!("expected RefundUnverified, got {other:?}"),
        }
    }

    #[test]
    fn deposit_gas_limit_is_the_larger_funded_deposit() {
        let gas_used = [
            include_str!("../../relay-fixtures/deposit_receipt_robinhood.json"),
            include_str!("../../relay-fixtures/deposit_receipt_ethereum.json"),
        ]
        .map(|body| {
            serde_json::from_str::<TransactionReceipt>(body)
                .unwrap()
                .gas_used
        });

        assert_eq!(gas_used, [57_114, 49_083]);
        assert_eq!(RELAY_DEPOSIT_GAS_LIMIT, 57_114);
    }

    #[test]
    fn hub_is_not_a_corridor_chain() {
        let error = RelayBridge::try_from_ctx(RelayCtx {
            chain: Chain::Ethereum,
            ethereum_wallet: offline_wallet(),
            chain_wallet: offline_wallet(),
            ethereum_confirmations: CONFIRMATIONS,
            chain_confirmations: CONFIRMATIONS,
        })
        .err()
        .unwrap();

        assert!(
            matches!(error, RelayBridgeError::HubAsCorridorChain),
            "{error:?}"
        );
    }

    #[test]
    fn zero_confirmations_on_either_end_are_refused() {
        let refused = |ethereum_confirmations, chain_confirmations| {
            RelayBridge::try_from_ctx(RelayCtx {
                chain: Chain::Robinhood,
                ethereum_wallet: offline_wallet(),
                chain_wallet: offline_wallet(),
                ethereum_confirmations,
                chain_confirmations,
            })
            .err()
            .unwrap()
        };

        let hub = refused(0, CONFIRMATIONS);
        let chain = refused(CONFIRMATIONS, 0);

        assert!(
            matches!(
                hub,
                RelayBridgeError::ZeroConfirmations {
                    chain: Chain::Ethereum
                }
            ),
            "{hub:?}"
        );
        assert!(
            matches!(
                chain,
                RelayBridgeError::ZeroConfirmations {
                    chain: Chain::Robinhood
                }
            ),
            "{chain:?}"
        );
    }

    /// A refund is proven against our wallet on the side it lands on, and a
    /// quote names one refund address, so both ends need one address.
    #[test]
    fn wallets_with_different_addresses_are_refused() {
        let provider = ProviderBuilder::new()
            .connect_http("http://127.0.0.1:1".parse().unwrap())
            .erased();
        let other = RawPrivateKeyWallet::new(&B256::repeat_byte(0x02), provider, 1).unwrap();
        let (hub, chain) = (offline_wallet().address(), other.address());

        let error = RelayBridge::try_from_ctx(RelayCtx {
            chain: Chain::Robinhood,
            ethereum_wallet: offline_wallet(),
            chain_wallet: other,
            ethereum_confirmations: CONFIRMATIONS,
            chain_confirmations: CONFIRMATIONS,
        })
        .err()
        .unwrap();

        assert!(
            matches!(
                error,
                RelayBridgeError::WalletsDiffer { hub: refused_hub, chain: refused_chain }
                    if refused_hub == hub && refused_chain == chain
            ),
            "{error:?}"
        );
    }

    #[test]
    fn chain_without_a_depository_is_refused() {
        let error = RelayBridge::try_from_ctx(RelayCtx {
            chain: Chain::Base,
            ethereum_wallet: offline_wallet(),
            chain_wallet: offline_wallet(),
            ethereum_confirmations: CONFIRMATIONS,
            chain_confirmations: CONFIRMATIONS,
        })
        .err()
        .unwrap();

        assert!(
            matches!(error, RelayBridgeError::NoDepository { chain: Chain::Base }),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn prepared_pair_holds_consecutive_nonces_and_pinned_deposit_gas() {
        let harness = Harness::new().await;
        let quote = accepted(quote(&harness.chain, B256::random(), true));

        let prepared = harness
            .bridge
            .prepare_deposit(HopDirection::ToHub, &quote)
            .await
            .map(deposit_pair)
            .unwrap();

        let approve = prepared.approve.unwrap();
        assert_eq!(approve.nonce() + 1, prepared.deposit.nonce());
        assert_eq!(
            gas_limit(&prepared.deposit),
            85_671,
            "57,114 padded by half"
        );
        assert_eq!(approve.to(), Some(harness.chain.stable));
        assert_eq!(prepared.deposit.to(), Some(harness.chain.depository));
    }

    #[tokio::test]
    async fn pair_split_by_another_send_sends_its_approve_alone_and_a_retry_signs_only_the_deposit()
    {
        let harness = Harness::new().await;
        let bridge = pair_bridge(&harness, OnDepositSigning::AnotherSendFirst);
        let quote = accepted(quote(&harness.chain, B256::random(), true));

        let split = bridge
            .prepare_deposit(HopDirection::ToHub, &quote)
            .await
            .unwrap();
        let PreparedSwap::ApproveOnly { approve } = split else {
            panic!("expected ApproveOnly, got {split:?}");
        };
        let sent = bridge
            .broadcast_approve(HopDirection::ToHub, &approve)
            .await
            .unwrap();
        harness.chain.mine(CONFIRMATIONS - 1).await;
        bridge
            .confirm_approve(HopDirection::ToHub, &approve)
            .await
            .unwrap();
        let retry = bridge
            .prepare_deposit(HopDirection::ToHub, &quote)
            .await
            .map(deposit_pair)
            .unwrap();

        assert_eq!(approve.nonce(), 0);
        assert_eq!(sent, approve.tx_hash());
        assert_eq!(bridge.chain.wallet.transaction_submission(sent), None);
        assert_eq!(
            harness
                .chain
                .allowance(harness.chain.wallet(), harness.chain.depository)
                .await,
            AMOUNT
        );
        assert_eq!(
            retry.approve, None,
            "the approve sent alone already allows the deposit"
        );
        assert_eq!(
            retry.deposit.nonce(),
            2,
            "the other send still holds nonce 1"
        );
    }

    #[tokio::test]
    async fn failed_deposit_signing_releases_its_approve() {
        let harness = Harness::new().await;
        let bridge = pair_bridge(&harness, OnDepositSigning::Fail);
        let quote = accepted(quote(&harness.chain, B256::random(), true));

        let error = bridge
            .prepare_deposit(HopDirection::ToHub, &quote)
            .await
            .unwrap_err();

        assert!(
            matches!(error, RelayBridgeError::Evm(EvmError::Reverted { .. })),
            "{error:?}"
        );
        assert_eq!(next_two_nonces(&bridge.chain.wallet).await, [0, 1]);
    }

    #[tokio::test]
    async fn discarded_swap_frees_its_nonces() {
        let harness = Harness::new().await;
        let quote = accepted(quote(&harness.chain, B256::random(), true));
        let prepared = harness
            .bridge
            .prepare_deposit(HopDirection::ToHub, &quote)
            .await
            .unwrap();

        harness
            .bridge
            .discard_prepared(HopDirection::ToHub, &prepared)
            .await;

        assert_eq!(next_two_nonces(&harness.bridge.chain.wallet).await, [0, 1]);
    }

    #[tokio::test]
    async fn restored_swap_keeps_its_nonces_after_a_restart() {
        let harness = Harness::new().await;
        let quote = accepted(quote(&harness.chain, B256::random(), true));
        let prepared = harness
            .bridge
            .prepare_deposit(HopDirection::ToHub, &quote)
            .await
            .unwrap();
        let restarted = RelayBridge::try_from_ctx(RelayCtx {
            chain: Chain::Robinhood,
            ethereum_wallet: wallet(&harness.hub),
            chain_wallet: wallet(&harness.chain),
            ethereum_confirmations: CONFIRMATIONS,
            chain_confirmations: CONFIRMATIONS,
        })
        .unwrap()
        .with_local_contracts(contracts(&harness.hub), contracts(&harness.chain));

        restarted
            .restore_prepared(HopDirection::ToHub, &prepared)
            .await;

        assert_eq!(next_two_nonces(&restarted.chain.wallet).await, [2, 3]);
    }

    #[tokio::test]
    async fn deposit_broadcasts_idempotently_and_confirms_its_order() {
        let harness = Harness::new().await;
        harness.chain.mint(harness.chain.wallet(), AMOUNT).await;
        let order_id = B256::random();
        let quote = accepted(quote(&harness.chain, order_id, true));

        let prepared = harness
            .bridge
            .prepare_deposit(HopDirection::ToHub, &quote)
            .await
            .map(deposit_pair)
            .unwrap();
        let first = harness
            .bridge
            .broadcast_deposit(HopDirection::ToHub, &prepared)
            .await
            .unwrap();
        let again = harness
            .bridge
            .broadcast_deposit(HopDirection::ToHub, &prepared)
            .await
            .unwrap();
        harness.chain.mine(CONFIRMATIONS - 1).await;

        let deposit = harness
            .bridge
            .confirm_deposit(HopDirection::ToHub, RelayOrderId(order_id), &prepared, &[])
            .await
            .unwrap();

        assert_eq!(first, prepared.deposit.tx_hash());
        assert_eq!(again, first);
        assert_eq!(deposit.tx, first);
        assert_eq!(deposit.order_id, RelayOrderId(order_id));
        assert_eq!(deposit.amount, AMOUNT);
        assert_eq!(
            harness.chain.balance(harness.chain.depository).await,
            AMOUNT
        );
    }

    /// A mined deposit proves its approve's nonce used, so confirming it
    /// releases both reservations.
    #[tokio::test]
    async fn confirmed_deposit_releases_its_approve_nonce() {
        let harness = Harness::new().await;
        harness.chain.mint(harness.chain.wallet(), AMOUNT).await;
        let order_id = B256::random();
        let quote = accepted(quote(&harness.chain, order_id, true));
        let prepared = harness
            .bridge
            .prepare_deposit(HopDirection::ToHub, &quote)
            .await
            .map(deposit_pair)
            .unwrap();
        let approve = prepared.approve.clone().unwrap().tx_hash();
        let wallet = &harness.bridge.chain.wallet;
        assert_eq!(
            wallet
                .transaction_submission(approve)
                .map(|submission| submission.nonce),
            Some(0)
        );

        let deposit = harness
            .bridge
            .broadcast_deposit(HopDirection::ToHub, &prepared)
            .await
            .unwrap();
        harness.chain.mine(CONFIRMATIONS - 1).await;
        harness
            .bridge
            .confirm_deposit(HopDirection::ToHub, RelayOrderId(order_id), &prepared, &[])
            .await
            .unwrap();

        assert_eq!(wallet.transaction_submission(approve), None);
        assert_eq!(wallet.transaction_submission(deposit), None);
    }

    /// An approve sent alone after a split sits below the deposit signed
    /// next, so that deposit's confirmation releases it too.
    #[tokio::test]
    async fn confirmed_deposit_releases_the_split_approves_below_it() {
        let harness = Harness::new().await;
        harness.chain.mint(harness.chain.wallet(), AMOUNT).await;
        let order_id = B256::random();
        let quote = accepted(quote(&harness.chain, order_id, true));
        let wallet = &harness.bridge.chain.wallet;
        let split = wallet
            .prepare_pending(
                harness.chain.stable,
                quote.approve().unwrap().data.clone(),
                "split approve",
            )
            .await
            .unwrap();
        harness
            .bridge
            .broadcast_approve(HopDirection::ToHub, &split)
            .await
            .unwrap();
        let pair = harness
            .bridge
            .prepare_deposit(HopDirection::ToHub, &quote)
            .await
            .map(deposit_pair)
            .unwrap();
        harness
            .bridge
            .broadcast_deposit(HopDirection::ToHub, &pair)
            .await
            .unwrap();
        harness.chain.mine(CONFIRMATIONS - 1).await;
        assert_eq!(
            wallet
                .transaction_submission(split.tx_hash())
                .map(|submission| submission.nonce),
            Some(0)
        );

        harness
            .bridge
            .confirm_deposit(
                HopDirection::ToHub,
                RelayOrderId(order_id),
                &pair,
                std::slice::from_ref(&split),
            )
            .await
            .unwrap();

        assert_eq!(pair.approve, None);
        assert_eq!(pair.deposit.nonce(), 1);
        assert_eq!(wallet.transaction_submission(split.tx_hash()), None);
    }

    #[tokio::test]
    async fn deposit_and_payment_short_of_the_bridge_confirmations_are_unconfirmed() {
        let harness = Harness::new().await;
        harness.chain.mint(harness.chain.wallet(), AMOUNT).await;
        harness.hub.mint(harness.hub.solver(), MINIMUM_OUT).await;
        let order_id = B256::random();
        let quote = accepted(quote(&harness.chain, order_id, true));
        let prepared = harness
            .bridge
            .prepare_deposit(HopDirection::ToHub, &quote)
            .await
            .map(deposit_pair)
            .unwrap();
        let deposit = harness
            .bridge
            .broadcast_deposit(HopDirection::ToHub, &prepared)
            .await
            .unwrap();
        let fill = harness
            .hub
            .pay(harness.hub.wallet(), MINIMUM_OUT, order_id)
            .await;

        let confirm = harness
            .bridge
            .confirm_deposit(HopDirection::ToHub, quote.order_id, &prepared, &[])
            .await
            .unwrap_err();
        let verify = harness
            .bridge
            .verify_fill(HopDirection::ToHub, quote.order_id, MINIMUM_OUT, &[fill])
            .await
            .unwrap_err();

        assert!(
            matches!(
                confirm,
                RelayBridgeError::Unconfirmed { tx, confirmations: 1, required: CONFIRMATIONS }
                    if tx == deposit
            ),
            "{confirm:?}"
        );
        assert!(
            matches!(
                verify,
                RelayBridgeError::Unconfirmed { tx, confirmations: 1, required: CONFIRMATIONS }
                    if tx == fill
            ),
            "{verify:?}"
        );
    }

    #[tokio::test]
    async fn receipt_from_a_reorged_out_block_is_not_canonical() {
        let harness = Harness::new().await;
        let bridge = hooked_bridge(&harness, None, true);
        let order_id = B256::random();
        let refunded = AMOUNT - U256::from(4_846);
        let pair = harness.send_deposit_to_hub(order_id).await;
        let deposit = pair.deposit.tx_hash();
        harness.chain.mint(harness.chain.solver(), refunded).await;
        let refund = harness
            .chain
            .pay(harness.chain.wallet(), refunded, order_id)
            .await;
        harness.chain.mine(CONFIRMATIONS - 1).await;

        let confirm = bridge
            .confirm_deposit(HopDirection::ToHub, RelayOrderId(order_id), &pair, &[])
            .await
            .unwrap_err();
        let verify = bridge
            .verify_refund(
                HopDirection::ToHub,
                RelayOrderId(order_id),
                AMOUNT,
                &[refund],
            )
            .await
            .unwrap_err();

        assert!(
            matches!(confirm, RelayBridgeError::NotCanonical { tx, .. } if tx == deposit),
            "{confirm:?}"
        );
        assert!(
            matches!(verify, RelayBridgeError::NotCanonical { tx, .. } if tx == refund),
            "{verify:?}"
        );
    }

    #[tokio::test]
    async fn quote_without_approve_signs_one_only_when_the_allowance_is_short() {
        let harness = Harness::new().await;
        let quote = accepted(quote(&harness.chain, B256::random(), false));

        let short = harness
            .bridge
            .prepare_deposit(HopDirection::ToHub, &quote)
            .await
            .map(deposit_pair)
            .unwrap();
        let approve = short.approve.unwrap();
        assert_eq!(approve.to(), Some(harness.chain.stable));
        assert_eq!(
            approve.input().unwrap(),
            Bytes::from(
                IERC20::approveCall {
                    spender: harness.chain.depository,
                    amount: AMOUNT,
                }
                .abi_encode()
            )
        );

        let pair = PreparedSwapDeposit {
            approve: Some(approve),
            deposit: short.deposit,
        };
        let deposit = harness
            .bridge
            .broadcast_deposit(HopDirection::ToHub, &pair)
            .await
            .unwrap();
        harness.chain.mine(CONFIRMATIONS - 1).await;
        let reverted = harness
            .bridge
            .confirm_deposit(HopDirection::ToHub, quote.order_id, &pair, &[])
            .await
            .unwrap_err();
        assert!(
            matches!(reverted, RelayBridgeError::DepositReverted { tx } if tx == deposit),
            "{reverted:?}"
        );
        harness.chain.mine(1).await;
        assert_eq!(
            harness
                .chain
                .allowance(harness.chain.wallet(), harness.chain.depository)
                .await,
            AMOUNT,
            "the deposit reverted unfunded, so the approve stands"
        );

        let covered = harness
            .bridge
            .prepare_deposit(HopDirection::ToHub, &quote)
            .await
            .map(deposit_pair)
            .unwrap();
        assert_eq!(covered.approve, None);
    }

    #[tokio::test]
    async fn deposit_crediting_another_depositor_is_refused() {
        let harness = Harness::new().await;
        let mut quote = quote(&harness.chain, B256::random(), true);
        let other = Address::repeat_byte(0x22);
        quote.deposit.data = Bytes::from(
            depositErc20Call {
                depositor: other,
                token: harness.chain.stable,
                amount: AMOUNT,
                id: B256::random(),
            }
            .abi_encode(),
        );

        let error = harness
            .bridge
            .prepare_deposit(HopDirection::ToHub, &accepted(quote))
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                RelayBridgeError::Quote(QuoteMismatch::AddressMismatch {
                    field: QuoteField::Depositor,
                    actual,
                    ..
                }) if actual == other
            ),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn quote_paying_another_recipient_is_refused_at_signing() {
        let harness = Harness::new().await;
        let mut quote = quote(&harness.chain, B256::random(), true);
        let other = Address::repeat_byte(0x33);
        quote.recipient = other;

        let error = harness
            .bridge
            .prepare_deposit(HopDirection::ToHub, &accepted(quote))
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                RelayBridgeError::Quote(QuoteMismatch::AddressMismatch {
                    field: QuoteField::Recipient,
                    expected,
                    actual,
                }) if expected == harness.hub.wallet() && actual == other
            ),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn quote_refunding_another_address_is_refused_at_signing() {
        let harness = Harness::new().await;
        let mut quote = quote(&harness.chain, B256::random(), true);
        let other = Address::repeat_byte(0x33);
        quote.refund_to = other;

        let error = harness
            .bridge
            .prepare_deposit(HopDirection::ToHub, &accepted(quote))
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                RelayBridgeError::Quote(QuoteMismatch::AddressMismatch {
                    field: QuoteField::RefundRecipient,
                    actual,
                    ..
                }) if actual == other
            ),
            "{error:?}"
        );
    }

    /// A quote is signed only for the direction it was quoted: its steps
    /// matching one end is not enough.
    #[tokio::test]
    async fn quote_for_the_reverse_direction_is_refused_at_signing() {
        let harness = Harness::new().await;
        let mut reversed = quote(&harness.chain, B256::random(), true);
        reversed.origin = Chain::Ethereum;
        reversed.destination = Chain::Robinhood;
        let to_hub = quote(&harness.chain, B256::random(), true);

        let reversed_quote = harness
            .bridge
            .prepare_deposit(HopDirection::ToHub, &accepted(reversed))
            .await
            .unwrap_err();
        let reversed_call = harness
            .bridge
            .prepare_deposit(HopDirection::FromHub, &accepted(to_hub))
            .await
            .unwrap_err();

        assert!(
            matches!(
                reversed_quote,
                RelayBridgeError::DirectionMismatch {
                    direction: HopDirection::ToHub,
                    origin: Chain::Ethereum,
                    destination: Chain::Robinhood,
                }
            ),
            "{reversed_quote:?}"
        );
        assert!(
            matches!(
                reversed_call,
                RelayBridgeError::DirectionMismatch {
                    direction: HopDirection::FromHub,
                    origin: Chain::Robinhood,
                    destination: Chain::Ethereum,
                }
            ),
            "{reversed_call:?}"
        );
        assert_eq!(
            next_two_nonces(&harness.bridge.chain.wallet).await,
            [0, 1],
            "nothing was signed"
        );
    }

    /// Why `prepare_deposit` refuses a quote for `order_id` whose step calldata
    /// is `approve` and `deposit`.
    async fn calldata_refusal(
        harness: &Harness,
        order_id: B256,
        approve: IERC20::approveCall,
        deposit: depositErc20Call,
    ) -> QuoteMismatch {
        let mut quote = quote(&harness.chain, order_id, true);
        quote.approve.as_mut().unwrap().data = Bytes::from(approve.abi_encode());
        quote.deposit.data = Bytes::from(deposit.abi_encode());

        match harness
            .bridge
            .prepare_deposit(HopDirection::ToHub, &accepted(quote))
            .await
        {
            Err(RelayBridgeError::Quote(mismatch)) => mismatch,
            other => panic!("expected a quote mismatch, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn step_calldata_off_the_quote_is_refused_at_signing() {
        let harness = Harness::new().await;
        let order_id = B256::random();
        let other = Address::repeat_byte(0x22);
        let approve = IERC20::approveCall {
            spender: harness.chain.depository,
            amount: AMOUNT,
        };
        let deposit = depositErc20Call {
            depositor: harness.chain.wallet(),
            token: harness.chain.stable,
            amount: AMOUNT,
            id: order_id,
        };

        let spender = calldata_refusal(
            &harness,
            order_id,
            IERC20::approveCall {
                spender: other,
                ..approve.clone()
            },
            deposit.clone(),
        )
        .await;
        let approved = calldata_refusal(
            &harness,
            order_id,
            IERC20::approveCall {
                amount: AMOUNT + U256::from(1),
                ..approve.clone()
            },
            deposit.clone(),
        )
        .await;
        let token = calldata_refusal(
            &harness,
            order_id,
            approve.clone(),
            depositErc20Call {
                token: other,
                ..deposit.clone()
            },
        )
        .await;
        let deposited = calldata_refusal(
            &harness,
            order_id,
            approve.clone(),
            depositErc20Call {
                amount: AMOUNT - U256::from(1),
                ..deposit.clone()
            },
        )
        .await;
        let order = calldata_refusal(
            &harness,
            order_id,
            approve,
            depositErc20Call {
                id: B256::ZERO,
                ..deposit
            },
        )
        .await;

        assert!(
            matches!(
                spender,
                QuoteMismatch::AddressMismatch { field: QuoteField::ApproveSpender, actual, .. }
                    if actual == other
            ),
            "{spender:?}"
        );
        assert!(
            matches!(
                approved,
                QuoteMismatch::AmountMismatch { field: QuoteField::ApproveAmount, expected, .. }
                    if expected == AMOUNT
            ),
            "{approved:?}"
        );
        assert!(
            matches!(
                token,
                QuoteMismatch::AddressMismatch { field: QuoteField::DepositToken, actual, .. }
                    if actual == other
            ),
            "{token:?}"
        );
        assert!(
            matches!(
                deposited,
                QuoteMismatch::AmountMismatch { field: QuoteField::DepositAmount, expected, .. }
                    if expected == AMOUNT
            ),
            "{deposited:?}"
        );
        assert!(
            matches!(
                order,
                QuoteMismatch::OrderIdMismatch { calldata, .. }
                    if calldata == RelayOrderId(B256::ZERO)
            ),
            "{order:?}"
        );
    }

    #[tokio::test]
    async fn deposit_step_for_another_contract_is_refused() {
        let harness = Harness::new().await;
        let mut quote = quote(&harness.chain, B256::random(), true);
        quote.deposit.to = harness.hub.depository;

        let error = harness
            .bridge
            .prepare_deposit(HopDirection::ToHub, &accepted(quote))
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                RelayBridgeError::Quote(QuoteMismatch::AddressMismatch {
                    field: QuoteField::DepositTarget,
                    actual,
                    ..
                }) if actual == harness.hub.depository
            ),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn deposit_step_for_another_chain_is_refused() {
        let harness = Harness::new().await;
        let mut quote = quote(&harness.chain, B256::random(), true);
        quote.deposit.chain_id = harness.hub.chain_id();

        let error = harness
            .bridge
            .prepare_deposit(HopDirection::ToHub, &accepted(quote))
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                RelayBridgeError::Quote(QuoteMismatch::StepChain {
                    step: QuoteStep::Deposit,
                    expected: 31_338,
                    actual: 31_337,
                })
            ),
            "{error:?}"
        );
    }

    /// The wallet signs each step with no native value, so a step asking for
    /// some is not the transaction Relay quoted.
    #[tokio::test]
    async fn step_sending_native_value_is_refused_at_signing() {
        let harness = Harness::new().await;
        let mut approve = quote(&harness.chain, B256::random(), true);
        approve.approve.as_mut().unwrap().value = U256::from(1);
        let mut deposit = quote(&harness.chain, B256::random(), true);
        deposit.deposit.value = U256::from(1);

        for (step, quote) in [(QuoteStep::Approve, approve), (QuoteStep::Deposit, deposit)] {
            let error = harness
                .bridge
                .prepare_deposit(HopDirection::ToHub, &accepted(quote))
                .await
                .unwrap_err();

            assert!(
                matches!(
                    error,
                    RelayBridgeError::Quote(QuoteMismatch::StepValue { step: refused, value })
                        if refused == step && value == U256::from(1)
                ),
                "{step:?}: {error:?}"
            );
        }
    }

    #[tokio::test]
    async fn fill_with_trailing_order_id_settles() {
        let harness = Harness::new().await;
        let order_id = B256::random();
        harness.hub.mint(harness.hub.solver(), MINIMUM_OUT).await;

        let tx = harness
            .hub
            .pay(harness.hub.wallet(), MINIMUM_OUT, order_id)
            .await;

        harness.hub.mine(CONFIRMATIONS - 1).await;

        let payment = harness
            .bridge
            .verify_fill(
                HopDirection::ToHub,
                RelayOrderId(order_id),
                MINIMUM_OUT,
                &[tx],
            )
            .await
            .unwrap();

        assert_eq!(
            payment,
            SwapPayment {
                tx,
                side: SwapSide::Destination,
                amount: MINIMUM_OUT,
            }
        );
        assert_eq!(harness.hub.balance(harness.hub.wallet()).await, MINIMUM_OUT);
    }

    #[tokio::test]
    async fn fill_from_the_hub_lands_on_the_corridor_chain() {
        let harness = Harness::new().await;
        let order_id = B256::random();
        harness
            .chain
            .mint(harness.chain.solver(), MINIMUM_OUT)
            .await;

        let tx = harness
            .chain
            .pay(harness.chain.wallet(), MINIMUM_OUT, order_id)
            .await;

        harness.chain.mine(CONFIRMATIONS - 1).await;

        let payment = harness
            .bridge
            .verify_fill(
                HopDirection::FromHub,
                RelayOrderId(order_id),
                MINIMUM_OUT,
                &[tx],
            )
            .await
            .unwrap();

        assert_eq!(payment.side, SwapSide::Destination);
        assert_eq!(payment.amount, MINIMUM_OUT);
    }

    #[tokio::test]
    async fn reverted_fill_is_unverified() {
        let harness = Harness::new().await;
        let order_id = B256::random();

        let tx = harness
            .hub
            .pay(harness.hub.wallet(), MINIMUM_OUT, order_id)
            .await;

        harness.hub.mine(CONFIRMATIONS - 1).await;

        let error = harness
            .bridge
            .verify_fill(
                HopDirection::ToHub,
                RelayOrderId(order_id),
                MINIMUM_OUT,
                &[tx],
            )
            .await
            .unwrap_err();

        assert_eq!(
            unverified_fill(error),
            (vec![tx], UnverifiedReason::Reverted)
        );
    }

    #[tokio::test]
    async fn refund_hash_reported_as_fill_is_unverified() {
        let harness = Harness::new().await;
        let order_id = B256::random();
        harness.chain.mint(harness.chain.solver(), AMOUNT).await;

        let refund = harness
            .chain
            .pay(harness.chain.wallet(), AMOUNT, order_id)
            .await;

        harness.chain.mine(CONFIRMATIONS - 1).await;

        let error = harness
            .bridge
            .verify_fill(
                HopDirection::ToHub,
                RelayOrderId(order_id),
                MINIMUM_OUT,
                &[refund],
            )
            .await
            .unwrap_err();

        assert_eq!(
            unverified_fill(error),
            (vec![refund], UnverifiedReason::OnOriginChain)
        );
    }

    /// A fill is never on the origin chain, so a refund reported as one is
    /// refused at once, not retried until it has its confirmations.
    #[tokio::test]
    async fn unconfirmed_refund_reported_as_fill_is_unverified_at_once() {
        let harness = Harness::new().await;
        let order_id = B256::random();
        harness.chain.mint(harness.chain.solver(), AMOUNT).await;
        let refund = harness
            .chain
            .pay(harness.chain.wallet(), AMOUNT, order_id)
            .await;

        let error = harness
            .bridge
            .verify_fill(
                HopDirection::ToHub,
                RelayOrderId(order_id),
                MINIMUM_OUT,
                &[refund],
            )
            .await
            .unwrap_err();

        assert_eq!(
            unverified_fill(error),
            (vec![refund], UnverifiedReason::OnOriginChain)
        );
    }

    /// Two ends on one node see every tx on both chains, so its side is
    /// ambiguous.
    #[tokio::test]
    async fn payment_on_both_chains_is_unverified() {
        let harness = Harness::new().await;
        let bridge = RelayBridge::try_from_ctx(RelayCtx {
            chain: Chain::Robinhood,
            ethereum_wallet: wallet(&harness.chain),
            chain_wallet: wallet(&harness.chain),
            ethereum_confirmations: CONFIRMATIONS,
            chain_confirmations: CONFIRMATIONS,
        })
        .unwrap()
        .with_local_contracts(contracts(&harness.chain), contracts(&harness.chain));
        let order_id = B256::random();
        harness.chain.mint(harness.chain.solver(), AMOUNT).await;
        let payment = harness
            .chain
            .pay(harness.chain.wallet(), AMOUNT, order_id)
            .await;
        harness.chain.mine(CONFIRMATIONS - 1).await;

        let error = bridge
            .verify_fill(
                HopDirection::ToHub,
                RelayOrderId(order_id),
                MINIMUM_OUT,
                &[payment],
            )
            .await
            .unwrap_err();

        assert_eq!(
            unverified_fill(error),
            (vec![payment], UnverifiedReason::OnBothChains)
        );
    }

    #[tokio::test]
    async fn fill_with_two_txs_is_ambiguous() {
        let harness = Harness::new().await;
        let order_id = B256::random();
        harness
            .hub
            .mint(harness.hub.solver(), MINIMUM_OUT * U256::from(2))
            .await;

        let first = harness
            .hub
            .pay(harness.hub.wallet(), MINIMUM_OUT, order_id)
            .await;

        harness.hub.mine(CONFIRMATIONS - 1).await;
        let second = harness
            .hub
            .pay(harness.hub.wallet(), MINIMUM_OUT, order_id)
            .await;
        harness.hub.mine(CONFIRMATIONS - 1).await;

        let error = harness
            .bridge
            .verify_fill(
                HopDirection::ToHub,
                RelayOrderId(order_id),
                MINIMUM_OUT,
                &[first, second],
            )
            .await
            .unwrap_err();

        assert_eq!(
            unverified_fill(error),
            (vec![first, second], UnverifiedReason::TxCount { count: 2 })
        );
    }

    #[tokio::test]
    async fn transfer_amount_differing_from_calldata_is_unverified() {
        let harness = Harness::new().await;
        let order_id = B256::random();
        harness.hub.mint(harness.hub.solver(), MINIMUM_OUT).await;
        harness.hub.set_transfer_fee(U256::from(1)).await;

        let tx = harness
            .hub
            .pay(harness.hub.wallet(), MINIMUM_OUT, order_id)
            .await;

        harness.hub.mine(CONFIRMATIONS - 1).await;

        let error = harness
            .bridge
            .verify_fill(
                HopDirection::ToHub,
                RelayOrderId(order_id),
                MINIMUM_OUT,
                &[tx],
            )
            .await
            .unwrap_err();

        assert_eq!(
            unverified_fill(error),
            (
                vec![tx],
                UnverifiedReason::TransferAmount {
                    calldata: MINIMUM_OUT,
                    logged: MINIMUM_OUT - U256::from(1),
                }
            )
        );
    }

    #[tokio::test]
    async fn fill_below_floor_is_refused() {
        let harness = Harness::new().await;
        let order_id = B256::random();
        let short = MINIMUM_OUT - U256::from(1);
        harness.hub.mint(harness.hub.solver(), short).await;

        let tx = harness.hub.pay(harness.hub.wallet(), short, order_id).await;

        harness.hub.mine(CONFIRMATIONS - 1).await;

        let error = harness
            .bridge
            .verify_fill(
                HopDirection::ToHub,
                RelayOrderId(order_id),
                MINIMUM_OUT,
                &[tx],
            )
            .await
            .unwrap_err();

        assert_eq!(
            unverified_fill(error),
            (
                vec![tx],
                UnverifiedReason::BelowMinimum {
                    amount: short,
                    minimum: MINIMUM_OUT,
                }
            )
        );
    }

    #[tokio::test]
    async fn fill_on_neither_chain_is_not_found() {
        let harness = Harness::new().await;
        let tx = TxHash::random();

        let error = harness
            .bridge
            .verify_fill(
                HopDirection::ToHub,
                RelayOrderId(B256::random()),
                MINIMUM_OUT,
                &[tx],
            )
            .await
            .unwrap_err();

        assert!(
            matches!(error, RelayBridgeError::TxNotFound { tx: missing } if missing == tx),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn zero_payment_is_neither_a_fill_nor_a_refund() {
        let harness = Harness::new().await;
        let order_id = B256::random();

        let tx = harness
            .hub
            .pay(harness.hub.wallet(), U256::ZERO, order_id)
            .await;

        harness.hub.mine(CONFIRMATIONS - 1).await;

        let fill = harness
            .bridge
            .verify_fill(
                HopDirection::ToHub,
                RelayOrderId(order_id),
                U256::ZERO,
                &[tx],
            )
            .await
            .unwrap_err();
        let refund = harness
            .bridge
            .verify_refund(HopDirection::ToHub, RelayOrderId(order_id), AMOUNT, &[tx])
            .await
            .unwrap_err();

        assert_eq!(
            unverified_fill(fill),
            (vec![tx], UnverifiedReason::ZeroAmount)
        );
        assert_eq!(
            unverified_refund(refund),
            (vec![tx], UnverifiedReason::ZeroAmount)
        );
    }

    #[tokio::test]
    async fn self_transfer_is_neither_a_fill_nor_a_refund() {
        let harness = Harness::new().await;
        let order_id = B256::random();
        let ours = harness.hub.wallet();
        harness.hub.mint(ours, MINIMUM_OUT).await;
        harness.hub.approve_relayer(ours).await;

        let tx = harness
            .hub
            .pay_from(ours, ours, MINIMUM_OUT, order_id)
            .await;

        harness.hub.mine(CONFIRMATIONS - 1).await;

        let fill = harness
            .bridge
            .verify_fill(
                HopDirection::ToHub,
                RelayOrderId(order_id),
                MINIMUM_OUT,
                &[tx],
            )
            .await
            .unwrap_err();
        let refund = harness
            .bridge
            .verify_refund(HopDirection::ToHub, RelayOrderId(order_id), AMOUNT, &[tx])
            .await
            .unwrap_err();

        assert_eq!(
            unverified_fill(fill),
            (vec![tx], UnverifiedReason::SelfTransfer)
        );
        assert_eq!(
            unverified_refund(refund),
            (vec![tx], UnverifiedReason::SelfTransfer)
        );
        assert_eq!(harness.hub.balance(ours).await, MINIMUM_OUT);
    }

    #[tokio::test]
    async fn refund_on_destination_chain_is_verified() {
        let harness = Harness::new().await;
        let order_id = B256::random();
        let refunded = AMOUNT - U256::from(4_846);
        harness.hub.mint(harness.hub.solver(), refunded).await;

        let tx = harness
            .hub
            .pay(harness.hub.wallet(), refunded, order_id)
            .await;

        harness.hub.mine(CONFIRMATIONS - 1).await;

        let payment = harness
            .bridge
            .verify_refund(HopDirection::ToHub, RelayOrderId(order_id), AMOUNT, &[tx])
            .await
            .unwrap();

        assert_eq!(
            payment,
            SwapPayment {
                tx,
                side: SwapSide::Destination,
                amount: refunded,
            }
        );
    }

    #[tokio::test]
    async fn refund_on_origin_chain_is_verified() {
        let harness = Harness::new().await;
        let order_id = B256::random();
        let refunded = AMOUNT - U256::from(4_846);
        harness.chain.mint(harness.chain.solver(), refunded).await;

        let tx = harness
            .chain
            .pay(harness.chain.wallet(), refunded, order_id)
            .await;

        harness.chain.mine(CONFIRMATIONS - 1).await;

        let payment = harness
            .bridge
            .verify_refund(HopDirection::ToHub, RelayOrderId(order_id), AMOUNT, &[tx])
            .await
            .unwrap();

        assert_eq!(
            payment,
            SwapPayment {
                tx,
                side: SwapSide::Origin,
                amount: refunded,
            }
        );
        assert_eq!(
            harness.chain.balance(harness.chain.wallet()).await,
            refunded
        );
    }

    #[tokio::test]
    async fn refund_above_the_deposit_is_unverified() {
        let harness = Harness::new().await;
        let order_id = B256::random();
        let deposited = AMOUNT - U256::from(1);
        harness.chain.mint(harness.chain.solver(), AMOUNT).await;

        let tx = harness
            .chain
            .pay(harness.chain.wallet(), AMOUNT, order_id)
            .await;

        harness.chain.mine(CONFIRMATIONS - 1).await;

        let error = harness
            .bridge
            .verify_refund(
                HopDirection::ToHub,
                RelayOrderId(order_id),
                deposited,
                &[tx],
            )
            .await
            .unwrap_err();

        assert_eq!(
            unverified_refund(error),
            (
                vec![tx],
                UnverifiedReason::AboveDeposit {
                    amount: AMOUNT,
                    deposited,
                }
            )
        );
    }

    #[tokio::test]
    async fn top_up_without_order_id_is_not_adopted() {
        let harness = Harness::new().await;
        let order_id = B256::random();
        let from_block = harness
            .bridge
            .origin_block(HopDirection::ToHub)
            .await
            .unwrap();

        let top_up = harness.chain.top_up_depository(AMOUNT).await;
        harness
            .chain
            .deposit_from_deployer(harness.chain.deployer(), AMOUNT, order_id)
            .await;
        harness.chain.mine(CONFIRMATIONS).await;

        let scan = harness
            .bridge
            .find_recent_deposits(HopDirection::ToHub, &[RelayOrderId(order_id)], from_block)
            .await
            .unwrap();
        let confirm = harness
            .bridge
            .confirm_deposit(
                HopDirection::ToHub,
                RelayOrderId(order_id),
                &sent_pair(&harness, top_up).await,
                &[],
            )
            .await
            .unwrap_err();

        assert_eq!(scan.deposits, vec![]);
        assert!(
            matches!(
                confirm,
                RelayBridgeError::DepositUnverified { tx, .. } if tx == top_up
            ),
            "{confirm:?}"
        );
    }

    #[tokio::test]
    async fn third_party_deposit_crediting_our_wallet_is_not_ours() {
        let harness = Harness::new().await;
        let order_id = B256::random();
        let from_block = harness
            .bridge
            .origin_block(HopDirection::ToHub)
            .await
            .unwrap();

        let spoof = harness
            .chain
            .deposit_from_deployer(harness.chain.wallet(), U256::from(1), order_id)
            .await;
        harness.chain.mine(CONFIRMATIONS).await;

        let scan = harness
            .bridge
            .find_recent_deposits(HopDirection::ToHub, &[RelayOrderId(order_id)], from_block)
            .await
            .unwrap();
        let confirm = harness
            .bridge
            .confirm_deposit(
                HopDirection::ToHub,
                RelayOrderId(order_id),
                &sent_pair(&harness, spoof).await,
                &[],
            )
            .await
            .unwrap_err();

        assert_eq!(scan.deposits, vec![]);
        assert!(
            matches!(
                confirm,
                RelayBridgeError::DepositUnverified { tx, .. } if tx == spoof
            ),
            "{confirm:?}"
        );
    }

    #[tokio::test]
    async fn escrow_scan_finds_unindexed_deposit_by_order_id() {
        let mut harness = Harness::new().await;
        harness.bridge = harness.bridge.with_log_chunk(3);
        let from_block = harness
            .bridge
            .origin_block(HopDirection::ToHub)
            .await
            .unwrap();
        harness.chain.mine(5).await;

        let ours = harness.deposit_to_hub(B256::random()).await;
        harness.chain.mine(7).await;
        let other = harness.deposit_to_hub(B256::random()).await;
        harness.chain.mine(4).await;
        let head = harness
            .bridge
            .origin_block(HopDirection::ToHub)
            .await
            .unwrap();

        let scan = harness
            .bridge
            .find_recent_deposits(HopDirection::ToHub, &[ours.order_id], from_block)
            .await
            .unwrap();
        let both = harness
            .bridge
            .find_recent_deposits(
                HopDirection::ToHub,
                &[other.order_id, ours.order_id],
                from_block,
            )
            .await
            .unwrap();

        assert_eq!(
            scan,
            DepositScan {
                deposits: vec![ours],
                scanned_to: head - (CONFIRMATIONS - 1),
            }
        );
        assert_eq!(both.deposits, vec![ours, other]);
    }

    #[tokio::test]
    async fn deposit_scan_finds_a_deposit_a_lagging_node_first_answers_empty() {
        let harness = Harness::new().await;
        let from_block = harness
            .bridge
            .origin_block(HopDirection::ToHub)
            .await
            .unwrap();
        let deposit = harness.deposit_to_hub(B256::random()).await;
        let bridge = scan_bridge(&harness, lagging_wallet(&harness.chain, 1));

        let scan = bridge
            .find_recent_deposits(HopDirection::ToHub, &[deposit.order_id], from_block)
            .await
            .unwrap();

        assert_eq!(scan.deposits, vec![deposit]);
    }

    #[tokio::test]
    async fn deposit_scan_refuses_a_deposit_log_from_a_reorged_out_block() {
        let harness = Harness::new().await;
        let from_block = harness
            .bridge
            .origin_block(HopDirection::ToHub)
            .await
            .unwrap();
        let deposit = harness.deposit_to_hub(B256::random()).await;
        let bridge = scan_bridge(&harness, stale_logs_wallet(&harness.chain));

        let error = bridge
            .find_recent_deposits(HopDirection::ToHub, &[deposit.order_id], from_block)
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                RelayBridgeError::NotCanonical { tx, block }
                    if tx == deposit.tx && block == deposit.block
            ),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn deposit_scan_covers_only_blocks_with_its_confirmations() {
        let harness = Harness::new().await;
        let from_block = harness
            .bridge
            .origin_block(HopDirection::ToHub)
            .await
            .unwrap();
        let order_id = B256::random();
        let pair = harness.send_deposit_to_hub(order_id).await;
        harness.chain.mine(CONFIRMATIONS - 2).await;

        let early = harness
            .bridge
            .find_recent_deposits(HopDirection::ToHub, &[RelayOrderId(order_id)], from_block)
            .await
            .unwrap();
        harness.chain.mine(1).await;
        let confirmed = harness
            .bridge
            .find_recent_deposits(HopDirection::ToHub, &[RelayOrderId(order_id)], from_block)
            .await
            .unwrap();
        let deposit = harness
            .bridge
            .confirm_deposit(HopDirection::ToHub, RelayOrderId(order_id), &pair, &[])
            .await
            .unwrap();

        assert_eq!(
            early,
            DepositScan {
                deposits: vec![],
                scanned_to: deposit.block - 1,
            }
        );
        assert_eq!(
            confirmed,
            DepositScan {
                deposits: vec![deposit],
                scanned_to: deposit.block,
            }
        );
    }

    #[tokio::test]
    async fn deposit_scan_from_an_unconfirmed_block_keeps_the_caller_cursor() {
        let harness = Harness::new().await;
        harness.chain.mine(CONFIRMATIONS).await;
        let head = harness
            .bridge
            .origin_block(HopDirection::ToHub)
            .await
            .unwrap();

        let scan = harness
            .bridge
            .find_recent_deposits(HopDirection::ToHub, &[], head)
            .await
            .unwrap();

        assert_eq!(
            scan,
            DepositScan {
                deposits: vec![],
                scanned_to: head - 1,
            }
        );
    }

    /// A cursor at `scanned_to + 1` passes the head when the chain needs one
    /// confirmation, or when a lagging node serves an older head: the scan
    /// reads nothing and leaves the cursor where it was.
    #[tokio::test]
    async fn deposit_scan_from_beyond_the_head_keeps_the_caller_cursor() {
        let harness = Harness::new().await;
        let head = harness
            .bridge
            .origin_block(HopDirection::ToHub)
            .await
            .unwrap();

        let next = harness
            .bridge
            .find_recent_deposits(HopDirection::ToHub, &[], head + 1)
            .await
            .unwrap();
        let far = harness
            .bridge
            .find_recent_deposits(HopDirection::ToHub, &[], head + 10)
            .await
            .unwrap();

        assert_eq!(
            next,
            DepositScan {
                deposits: vec![],
                scanned_to: head,
            }
        );
        assert_eq!(
            far,
            DepositScan {
                deposits: vec![],
                scanned_to: head + 9,
            }
        );
    }
}

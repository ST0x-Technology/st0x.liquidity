//! [`RelayBridge`]: the deposit we send through Relay's depository and the
//! proof of the fill or refund Relay's solver sends back.

use alloy::consensus::Transaction as _;
use alloy::primitives::{Address, Bytes, TxHash, U256};
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, Transaction};
use alloy::sol_types::{SolCall, SolEvent};
use alloy::transports::{RpcError, TransportErrorKind};
use async_trait::async_trait;
use tracing::{debug, info, warn};

use st0x_evm::{Chain, EvmError, IERC20, PreparedTransaction, Wallet};

use super::proof::{
    PaymentTerms, RelayErc20Deposit, UnverifiedReason, check_payment, deposit_event,
};
use super::quote::depositErc20Call;
use super::{QuoteStep, RelayOrderId, RelayQuote, StepTransaction};
use crate::{
    DepositScan, HopDirection, PreparedSwapDeposit, SwapBridge, SwapDeposit, SwapPayment, SwapSide,
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

/// Who builds a [`RelayBridge`]: the corridor chain and a wallet on each end,
/// with the confirmations each end's wallet waits for.
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
    log_chunk: u64,
}

/// One chain's stable, depository and our wallet there.
struct RelayEnd<W> {
    chain: Chain,
    stable: Address,
    depository: Address,
    wallet: W,
    /// A deposit scan covers only blocks with this many confirmations,
    /// counting the inclusion block as the wallet does.
    confirmations: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum RelayBridgeError {
    #[error("Relay has no depository pinned on {chain}")]
    NoDepository { chain: Chain },
    #[error("the corridor chain must not be the Ethereum hub")]
    HubAsCorridorChain,
    #[error("{chain} needs at least one confirmation, or a deposit scan reads the raw head")]
    ZeroConfirmations { chain: Chain },
    #[error("{step:?} step targets chain {step_chain}, the wallet is on chain {wallet_chain}")]
    StepChain {
        step: QuoteStep,
        step_chain: u64,
        wallet_chain: u64,
    },
    #[error("{step:?} step calls {actual}, expected {expected}")]
    StepTarget {
        step: QuoteStep,
        expected: Address,
        actual: Address,
    },
    #[error("deposit credits depositor {actual}, the signing wallet is {expected}")]
    Depositor { expected: Address, actual: Address },
    #[error("prepared approve has nonce {approve}, the deposit {deposit}: not consecutive")]
    PairNonces { approve: u64, deposit: u64 },
    #[error("deposit {tx} reverted")]
    DepositReverted { tx: TxHash },
    #[error("deposit {tx} emitted no deposit for order {order_id} from our wallet")]
    DepositUnverified { tx: TxHash, order_id: RelayOrderId },
    #[error("deposit log for order {order_id} carries no tx hash or block")]
    DepositLogIncomplete { order_id: RelayOrderId },
    #[error("deposit scan from block {from_block} is ahead of the head {head}")]
    ScanAheadOfHead { from_block: u64, head: u64 },
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

        Ok(Self {
            hub: RelayEnd::pinned(
                Chain::Ethereum,
                ctx.ethereum_wallet,
                ctx.ethereum_confirmations,
            )?,
            chain: RelayEnd::pinned(ctx.chain, ctx.chain_wallet, ctx.chain_confirmations)?,
            log_chunk: DEPOSIT_LOG_CHUNK,
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
        self.log_chunk = blocks;
        self
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

    /// Locates the one tx in `txs` and proves it paid `order_id`.
    async fn verify_payment(
        &self,
        direction: HopDirection,
        order_id: RelayOrderId,
        txs: &[TxHash],
    ) -> Result<SwapPayment, ProofError> {
        let [tx] = txs else {
            return Err(ProofError::Unverified(UnverifiedReason::TxCount {
                count: txs.len(),
            }));
        };

        let (side, found) = self
            .locate(direction, *tx)
            .await?
            .ok_or(ProofError::NotFound { tx: *tx })?;

        let amount = self.prove(direction, side, *tx, &found, order_id).await?;

        Ok(SwapPayment {
            tx: *tx,
            side,
            amount,
        })
    }
}

#[async_trait]
impl<EthWallet: Wallet, ChainWallet: Wallet> SwapBridge for RelayBridge<EthWallet, ChainWallet> {
    type Error = RelayBridgeError;
    type Quote = RelayQuote;
    type OrderId = RelayOrderId;

    async fn prepare_deposit(
        &self,
        direction: HopDirection,
        quote: &RelayQuote,
    ) -> Result<PreparedSwapDeposit, RelayBridgeError> {
        match direction {
            HopDirection::ToHub => self.chain.prepare_deposit(quote).await,
            HopDirection::FromHub => self.hub.prepare_deposit(quote).await,
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

    async fn confirm_deposit(
        &self,
        direction: HopDirection,
        order_id: RelayOrderId,
        deposit_tx: TxHash,
    ) -> Result<SwapDeposit<RelayOrderId>, RelayBridgeError> {
        match direction {
            HopDirection::ToHub => self.chain.confirm_deposit(order_id, deposit_tx).await,
            HopDirection::FromHub => self.hub.confirm_deposit(order_id, deposit_tx).await,
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
                    .find_deposits(order_ids, from_block, self.log_chunk)
                    .await
            }
            HopDirection::FromHub => {
                self.hub
                    .find_deposits(order_ids, from_block, self.log_chunk)
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

        let payment = self
            .verify_payment(direction, order_id, txs)
            .await
            .map_err(|error| error.into_bridge_error(unverified))?;

        if payment.side == SwapSide::Origin {
            return Err(unverified(UnverifiedReason::OnOriginChain));
        }

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

        let payment = self
            .verify_payment(direction, order_id, txs)
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
    /// none and the allowance falls short) and the deposit at the next nonces.
    /// Another send from this wallet between the two signs takes the nonce in
    /// between, so a pair that is not consecutive is discarded and refused.
    async fn prepare_deposit(
        &self,
        quote: &RelayQuote,
    ) -> Result<PreparedSwapDeposit, RelayBridgeError> {
        let wallet_chain = self.wallet.provider().get_chain_id().await?;

        self.check_step(QuoteStep::Deposit, &quote.deposit, wallet_chain)?;

        let depositor = depositErc20Call::abi_decode(&quote.deposit.data)
            .map_err(EvmError::from)?
            .depositor;
        if depositor != self.wallet.address() {
            return Err(RelayBridgeError::Depositor {
                expected: self.wallet.address(),
                actual: depositor,
            });
        }

        let approve = match &quote.approve {
            Some(step) => {
                self.check_step(QuoteStep::Approve, step, wallet_chain)?;
                Some(step.data.clone())
            }
            None => self.approve_if_short(quote.amounts.amount_in).await?,
        };

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
                quote.deposit.data.clone(),
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
                "Relay deposit pair is not consecutive, discarding both"
            );
            self.wallet.discard_prepared(deposit.tx_hash()).await;
            self.wallet.discard_prepared(approve.tx_hash()).await;
            return Err(error);
        }

        debug!(
            target: "bridge",
            chain = %self.chain,
            order_id = %quote.order_id,
            approve = ?approve.as_ref().map(PreparedTransaction::tx_hash),
            deposit = %deposit.tx_hash(),
            "Relay deposit signed"
        );

        Ok(PreparedSwapDeposit { approve, deposit })
    }

    /// The step must be for this wallet's chain and call this end's contract.
    fn check_step(
        &self,
        step: QuoteStep,
        transaction: &StepTransaction,
        wallet_chain: u64,
    ) -> Result<(), RelayBridgeError> {
        if transaction.chain_id != wallet_chain {
            return Err(RelayBridgeError::StepChain {
                step,
                step_chain: transaction.chain_id,
                wallet_chain,
            });
        }

        let expected = match step {
            QuoteStep::Approve => self.stable,
            QuoteStep::Deposit => self.depository,
        };

        if transaction.to != expected {
            return Err(RelayBridgeError::StepTarget {
                step,
                expected,
                actual: transaction.to,
            });
        }

        Ok(())
    }

    /// With no approve step, the standing allowance must still cover the
    /// deposit: another transfer from this wallet may have spent it.
    async fn approve_if_short(&self, amount: U256) -> Result<Option<Bytes>, RelayBridgeError> {
        let allowance = IERC20::new(self.stable, self.wallet.provider())
            .allowance(self.wallet.address(), self.depository)
            .call()
            .await
            .map_err(EvmError::from)?;

        if allowance >= amount {
            return Ok(None);
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

    async fn confirm_deposit(
        &self,
        order_id: RelayOrderId,
        tx: TxHash,
    ) -> Result<SwapDeposit<RelayOrderId>, RelayBridgeError> {
        let receipt = self.wallet.await_receipt(tx).await?;

        if !receipt.status() {
            return Err(RelayBridgeError::DepositReverted { tx });
        }

        let deposit = receipt
            .inner
            .logs()
            .iter()
            .filter_map(|log| deposit_event(log, self.depository))
            .find(|event| self.is_ours(event) && RelayOrderId(event.id) == order_id)
            .ok_or(RelayBridgeError::DepositUnverified { tx, order_id })?;

        let block = receipt
            .block_number
            .ok_or(RelayBridgeError::DepositUnverified { tx, order_id })?;

        Ok(SwapDeposit {
            tx,
            order_id,
            amount: deposit.amount,
            block,
        })
    }

    fn is_ours(&self, event: &RelayErc20Deposit) -> bool {
        event.from == self.wallet.address() && event.token == self.stable
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
        chunk: u64,
    ) -> Result<DepositScan<RelayOrderId>, RelayBridgeError> {
        let head = self.wallet.provider().get_block_number().await?;

        if from_block > head {
            return Err(RelayBridgeError::ScanAheadOfHead { from_block, head });
        }

        let confirmed = head.saturating_sub(self.confirmations.saturating_sub(1));
        let scanned_to = confirmed.max(from_block.saturating_sub(1));
        let mut deposits = Vec::new();
        let mut start = from_block;

        while start <= scanned_to {
            let end = start
                .saturating_add(chunk.saturating_sub(1))
                .min(scanned_to);

            let filter = Filter::new()
                .address(self.depository)
                .event_signature(RelayErc20Deposit::SIGNATURE_HASH)
                .from_block(start)
                .to_block(end);

            let logs = self.wallet.provider().get_logs(&filter).await?;

            for log in &logs {
                let Some(event) = deposit_event(log, self.depository) else {
                    continue;
                };
                let order_id = RelayOrderId(event.id);

                if !self.is_ours(&event) || !order_ids.contains(&order_id) {
                    continue;
                }

                let (Some(tx), Some(block)) = (log.transaction_hash, log.block_number) else {
                    return Err(RelayBridgeError::DepositLogIncomplete { order_id });
                };

                deposits.push(SwapDeposit {
                    tx,
                    order_id,
                    amount: event.amount,
                    block,
                });
            }

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

    /// Waits for `found` to reach this chain's confirmations, then checks it
    /// paid our wallet this end's stable for `order_id`.
    async fn prove(
        &self,
        tx: TxHash,
        found: &Transaction,
        order_id: RelayOrderId,
    ) -> Result<U256, ProofError> {
        let receipt = self.wallet.await_receipt(tx).await?;

        let terms = PaymentTerms {
            stable: self.stable,
            recipient: self.wallet.address(),
            order_id,
        };

        check_payment(&terms, found.to(), found.input(), &receipt).map_err(ProofError::Unverified)
    }
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

/// A proof that failed: a tx that is not the payment, or a read that did not
/// finish.
enum ProofError {
    Unverified(UnverifiedReason),
    NotFound { tx: TxHash },
    Evm(EvmError),
    Rpc(RpcError<TransportErrorKind>),
}

impl From<EvmError> for ProofError {
    fn from(error: EvmError) -> Self {
        Self::Evm(error)
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
            Self::Evm(error) => RelayBridgeError::Evm(error),
            Self::Rpc(error) => RelayBridgeError::Rpc(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use alloy::consensus::TxEnvelope;
    use alloy::eips::Decodable2718;
    use alloy::primitives::{B256, Signature};
    use alloy::providers::{DynProvider, ProviderBuilder};
    use alloy::rpc::types::TransactionReceipt;

    use st0x_evm::Evm;
    use st0x_evm::local::RawPrivateKeyWallet;

    use super::super::test_chain::RelayChain;
    use super::super::{BasisPoints, QuoteAmounts, QuoteFees, RelayRequestId};
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
            .with_local_contracts(contracts(&hub), contracts(&chain));

            Self { hub, chain, bridge }
        }

        /// Signs, broadcasts and confirms a Robinhood -> hub deposit.
        async fn deposit_to_hub(&self, order_id: B256) -> SwapDeposit<RelayOrderId> {
            self.chain.mint(self.chain.wallet(), AMOUNT).await;
            let quote = quote(&self.chain, order_id, true);

            let prepared = self
                .bridge
                .prepare_deposit(HopDirection::ToHub, &quote)
                .await
                .unwrap();
            let tx = self
                .bridge
                .broadcast_deposit(HopDirection::ToHub, &prepared)
                .await
                .unwrap();

            self.bridge
                .confirm_deposit(HopDirection::ToHub, quote.order_id, tx)
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

    /// A quote for depositing `AMOUNT` on `origin`, shaped like a checked
    /// live one.
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

    /// Our corridor-chain wallet, with a hook on the deposit's signing.
    struct PairWallet {
        inner: TestWallet,
        on_deposit: OnDepositSigning,
        stable: Address,
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
            match self.on_deposit {
                OnDepositSigning::AnotherSendFirst => {
                    self.inner
                        .prepare_pending(self.stable, probe_calldata(), "another send")
                        .await?;
                }
                OnDepositSigning::Fail => {
                    return Err(EvmError::Reverted {
                        tx_hash: TxHash::ZERO,
                    });
                }
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

    /// A bridge whose corridor-chain wallet runs `on_deposit` when it signs
    /// the deposit.
    fn pair_bridge(
        harness: &Harness,
        on_deposit: OnDepositSigning,
    ) -> RelayBridge<TestWallet, PairWallet> {
        RelayBridge::try_from_ctx(RelayCtx {
            chain: Chain::Robinhood,
            ethereum_wallet: wallet(&harness.hub),
            chain_wallet: PairWallet {
                inner: wallet(&harness.chain),
                on_deposit,
                stable: harness.chain.stable,
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
    async fn next_two_nonces(wallet: &PairWallet) -> [u64; 2] {
        let mut nonces = [0; 2];

        for nonce in &mut nonces {
            *nonce = wallet
                .prepare_pending(wallet.stable, probe_calldata(), "nonce probe")
                .await
                .unwrap()
                .nonce();
        }

        nonces
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
        let quote = quote(&harness.chain, B256::random(), true);

        let prepared = harness
            .bridge
            .prepare_deposit(HopDirection::ToHub, &quote)
            .await
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
    async fn pair_split_by_another_send_is_refused_and_releases_both_nonces() {
        let harness = Harness::new().await;
        let bridge = pair_bridge(&harness, OnDepositSigning::AnotherSendFirst);
        let quote = quote(&harness.chain, B256::random(), true);

        let error = bridge
            .prepare_deposit(HopDirection::ToHub, &quote)
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                RelayBridgeError::PairNonces {
                    approve: 0,
                    deposit: 2
                }
            ),
            "{error:?}"
        );
        assert_eq!(
            next_two_nonces(&bridge.chain.wallet).await,
            [0, 2],
            "the other send still holds nonce 1"
        );
    }

    #[tokio::test]
    async fn failed_deposit_signing_releases_its_approve() {
        let harness = Harness::new().await;
        let bridge = pair_bridge(&harness, OnDepositSigning::Fail);
        let quote = quote(&harness.chain, B256::random(), true);

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
    async fn deposit_broadcasts_idempotently_and_confirms_its_order() {
        let harness = Harness::new().await;
        harness.chain.mint(harness.chain.wallet(), AMOUNT).await;
        let order_id = B256::random();
        let quote = quote(&harness.chain, order_id, true);

        let prepared = harness
            .bridge
            .prepare_deposit(HopDirection::ToHub, &quote)
            .await
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

        let deposit = harness
            .bridge
            .confirm_deposit(HopDirection::ToHub, RelayOrderId(order_id), first)
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

    #[tokio::test]
    async fn quote_without_approve_signs_one_only_when_the_allowance_is_short() {
        let harness = Harness::new().await;
        let quote = quote(&harness.chain, B256::random(), false);

        let short = harness
            .bridge
            .prepare_deposit(HopDirection::ToHub, &quote)
            .await
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

        let deposit = harness
            .bridge
            .broadcast_deposit(
                HopDirection::ToHub,
                &PreparedSwapDeposit {
                    approve: Some(approve),
                    deposit: short.deposit,
                },
            )
            .await
            .unwrap();
        let reverted = harness
            .bridge
            .confirm_deposit(HopDirection::ToHub, quote.order_id, deposit)
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
            .prepare_deposit(HopDirection::ToHub, &quote)
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                RelayBridgeError::Depositor { actual, .. } if actual == other
            ),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn deposit_step_for_another_contract_is_refused() {
        let harness = Harness::new().await;
        let mut quote = quote(&harness.chain, B256::random(), true);
        quote.deposit.to = harness.hub.depository;

        let error = harness
            .bridge
            .prepare_deposit(HopDirection::ToHub, &quote)
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                RelayBridgeError::StepTarget { step: QuoteStep::Deposit, actual, .. }
                    if actual == harness.hub.depository
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
            .prepare_deposit(HopDirection::ToHub, &quote)
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                RelayBridgeError::StepChain {
                    step: QuoteStep::Deposit,
                    step_chain: 31_337,
                    wallet_chain: 31_338,
                }
            ),
            "{error:?}"
        );
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
        let second = harness
            .hub
            .pay(harness.hub.wallet(), MINIMUM_OUT, order_id)
            .await;

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
        harness.chain.deposit_from_deployer(AMOUNT, order_id).await;
        harness.chain.mine(CONFIRMATIONS).await;

        let scan = harness
            .bridge
            .find_recent_deposits(HopDirection::ToHub, &[RelayOrderId(order_id)], from_block)
            .await
            .unwrap();
        let confirm = harness
            .bridge
            .confirm_deposit(HopDirection::ToHub, RelayOrderId(order_id), top_up)
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
    async fn deposit_scan_covers_only_blocks_with_its_confirmations() {
        let harness = Harness::new().await;
        let from_block = harness
            .bridge
            .origin_block(HopDirection::ToHub)
            .await
            .unwrap();
        let deposit = harness.deposit_to_hub(B256::random()).await;
        harness.chain.mine(CONFIRMATIONS - 2).await;

        let early = harness
            .bridge
            .find_recent_deposits(HopDirection::ToHub, &[deposit.order_id], from_block)
            .await
            .unwrap();
        harness.chain.mine(1).await;
        let confirmed = harness
            .bridge
            .find_recent_deposits(HopDirection::ToHub, &[deposit.order_id], from_block)
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

    #[tokio::test]
    async fn deposit_scan_from_beyond_the_head_is_refused() {
        let harness = Harness::new().await;
        let head = harness
            .bridge
            .origin_block(HopDirection::ToHub)
            .await
            .unwrap();

        let error = harness
            .bridge
            .find_recent_deposits(HopDirection::ToHub, &[], head + 10)
            .await
            .unwrap_err();

        assert!(
            matches!(error, RelayBridgeError::ScanAheadOfHead { from_block, .. } if from_block == head + 10),
            "{error:?}"
        );
    }
}

//! Apalis job that records a confirmed bot-paid gas receipt cost.
//!
//! Enqueued by a consumer right after its existing on-chain confirmation step
//! succeeds (vault deposit/withdraw, wrap/unwrap, CCTP burn/mint, USDC
//! transfer). The consumer only holds a `TxHash` at that point (the domain
//! traits strip the receipt), so this job refetches the receipt by hash,
//! reads the block for its timestamp, values the gas in USD (see
//! `super::valuation`), and records the fact through `BotGasCostLedger`.
//!
//! Runs as a best-effort worker (see ADR 0020): a terminal failure
//! dead-letters that one receipt without blocking or slowing trading.

use alloy::primitives::{Address, TxHash};
use alloy::providers::{Provider, RootProvider};
use alloy::transports::{RpcError, TransportErrorKind};
use chrono::DateTime;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{error, warn};

use st0x_event_sorcery::SendError;
use st0x_evm::{Chain, Wallet};
use st0x_finance::Symbol;

use super::valuation::{EthUsdValuationError, read_eth_usd_price};
use super::{BotGasCostError, BotGasCostLedger, BotGasOperationCategory, BotGasReceiptCost};
use crate::conductor::job::{Job, JobQueue, Label, QueuePushError};

/// Persistent job queue for [`RecordBotGasReceiptCost`].
pub(crate) type RecordBotGasReceiptCostJobQueue = JobQueue<RecordBotGasReceiptCost>;

/// Enqueues [`RecordBotGasReceiptCost`] jobs from production confirm sites
/// (vault deposit/withdraw, wrap/unwrap, CCTP burn/mint, USDC transfer).
///
/// `Disabled` is for the two paths outside this design's scope (see ADR 0017
/// sign-off #5): the CLI `transfer-equity`/`transfer-usdc` commands (no apalis
/// worker infrastructure exists there) and the panicking service stubs used by
/// `transfer fail`/`transfer reconcile`, whose other services already panic
/// before an enqueue would be attempted. `Disabled` is a deliberate no-op, not
/// a swallowed failure.
///
/// Every `Enabled` site propagates enqueue errors to its caller, with two
/// caller-side responses: the USDC cross-venue transfer jobs, the
/// wrapped/unwrapped equity-recovery jobs, and the equity mint/redemption
/// transfer jobs all treat the error as non-terminal and delayed-redrive the
/// job (see each job module's `perform`) so the failure never consumes the
/// apalis retry budget or trips a supervised worker's fail-stop circuit (ADR
/// 0017 SS4). `EquityRedemption::SendTokens` is the one exception: it logs
/// and continues rather than propagating, because retrying the send would
/// risk sending tokens twice (see SPEC.md's bot-gas "Known gaps" for the
/// resulting permanent loss of that one cost fact).
#[derive(Clone)]
pub(crate) enum BotGasReceiptCostEnqueuer {
    Enabled(RecordBotGasReceiptCostJobQueue),
    Disabled,
}

impl BotGasReceiptCostEnqueuer {
    pub(crate) async fn enqueue(&self, job: RecordBotGasReceiptCost) -> Result<(), QueuePushError> {
        match self {
            Self::Enabled(queue) => {
                let mut queue = queue.clone();
                queue.push(job).await
            }
            Self::Disabled => {
                // Deliberate no-op for the two out-of-scope paths documented
                // above -- logged at warn so an accidentally-Disabled
                // production call site (e.g. a construction site that forgot
                // `.with_bot_gas_enqueuer(...)`) surfaces under prod log
                // filtering rather than silently dropping the cost fact.
                // Disabled is only legitimate on CLI paths, where the noise
                // is harmless.
                warn!(
                    target: "rebalance",
                    chain = %job.chain,
                    tx_hash = %job.tx_hash,
                    category = %job.category,
                    "bot-gas cost recording skipped: enqueuer is Disabled"
                );
                Ok(())
            }
        }
    }
}

/// Coarse, matchable classification of why an apalis push failed, captured
/// at the `QueuePushError` boundary so callers can branch on failure shape
/// even though the underlying non-`Clone`, non-serializable `sqlx::Error`
/// chain can't be threaded through (see [`BotGasEnqueueFailure`]'s doc).
/// Mirrors `apalis_core::backend::TaskSinkError`'s two variants: the push
/// itself failed (e.g. a closed/unreachable pool) vs. encoding/decoding the
/// task payload failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum QueuePushFailureKind {
    /// The underlying storage push failed (connection/pool error, etc).
    Push,
    /// Encoding or decoding the task payload failed.
    Codec,
}

impl From<&QueuePushError> for QueuePushFailureKind {
    fn from(error: &QueuePushError) -> Self {
        match &error.0 {
            apalis_core::backend::TaskSinkError::PushError(_) => Self::Push,
            apalis_core::backend::TaskSinkError::CodecError(_) => Self::Codec,
        }
    }
}

/// Shared error payload for a failed bot-gas receipt-cost enqueue, reused by
/// every aggregate that enqueues one after a confirm step (`EquityRedemption`,
/// `WrappedEquityRecovery`, `UnwrappedEquityRecovery`). Each of those call
/// sites is a local SQLite write via apalis, distinct from the onchain
/// operation it follows, and is safe to retry because the aggregate has not
/// advanced past the confirmed state yet.
///
/// `QueuePushError` can't be carried as a typed `#[source]`/`#[from]` field on
/// those aggregates' error enums: `EventSourced::Error` must satisfy
/// `st0x_event_sorcery::DomainError` (`Clone + Serialize + DeserializeOwned`),
/// which `QueuePushError` (wrapping a non-`Clone`, non-serializable
/// `sqlx::Error` chain) cannot implement. `kind` gives callers a matchable
/// failure shape without threading the original `sqlx::Error` through;
/// `message` (rendered via `Display` at this one shared boundary) remains
/// for diagnostics/logging, since it is strictly more detailed than `kind`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("Failed to enqueue bot-gas receipt cost recording for tx {tx_hash}: {message}")]
pub(crate) struct BotGasEnqueueFailure {
    pub(crate) tx_hash: TxHash,
    pub(crate) kind: QueuePushFailureKind,
    pub(crate) message: String,
}

impl BotGasEnqueueFailure {
    pub(crate) fn from_queue_push_error(tx_hash: TxHash, error: &QueuePushError) -> Self {
        Self {
            tx_hash,
            kind: QueuePushFailureKind::from(error),
            message: error.to_string(),
        }
    }
}

#[cfg(test)]
pub(super) mod test_support {
    use super::{BotGasEnqueueFailure, QueuePushFailureKind, TxHash};

    /// Builds a well-formed [`BotGasEnqueueFailure`] for tests outside this
    /// module. See [`super::super::test_bot_gas_enqueue_failure`].
    pub(crate) fn enqueue_failure(tx_hash: TxHash) -> BotGasEnqueueFailure {
        BotGasEnqueueFailure {
            tx_hash,
            kind: QueuePushFailureKind::Push,
            message: "test-induced enqueue failure".to_owned(),
        }
    }
}

/// Apalis job payload. Carries only what the worker cannot otherwise
/// recover: the aggregate's identity (`chain`, `tx_hash`) and the
/// classification the consumer already knows (`category`, `symbol`), plus
/// the bounded redrive counter described on [`MAX_REDRIVE_ATTEMPTS`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RecordBotGasReceiptCost {
    pub(crate) chain: Chain,
    pub(crate) tx_hash: TxHash,
    pub(crate) category: BotGasOperationCategory,
    pub(crate) symbol: Option<Symbol>,
    /// Number of times this job has already been delayed-redriven by
    /// [`RecordBotGasReceiptCost::redrive_or_dead_letter`]. `#[serde(default)]`
    /// so a job enqueued by an older binary (before this field existed)
    /// still deserializes, starting its budget fresh at 0.
    #[serde(default)]
    pub(crate) redrive_attempts: u32,
}

impl RecordBotGasReceiptCost {
    /// Builds a fresh (zero redrive attempts) job for a Base-chain tx.
    /// Every current production enqueue site is Base-only (vault
    /// deposit/withdraw, wrap/unwrap, USDC wallet transfer); the CCTP burn/
    /// mint's `Chain::Ethereum` job is constructed inline where it
    /// arises since it is the one exception.
    pub(crate) fn for_base_tx(
        tx_hash: TxHash,
        category: BotGasOperationCategory,
        symbol: Symbol,
    ) -> Self {
        Self {
            chain: Chain::Base,
            tx_hash,
            category,
            symbol: Some(symbol),
            redrive_attempts: 0,
        }
    }
}

/// Delay before re-enqueueing a `RecordBotGasReceiptCost` job after a
/// transient RPC-shaped condition: the receipt (or, once fetched, its block)
/// is not yet visible to the RPC endpoint, or an RPC call outright errors. A
/// load-balanced RPC provider may route a read to a node that has not caught
/// up to the confirming tx yet (see AGENTS.md's "Onchain Transaction
/// Confirmations" note); this rides out that lag instead of burning the
/// job's fixed 3-attempt apalis retry budget (~7s at the configured backoff)
/// and permanently dropping the cost fact.
const REDRIVE_DELAY: std::time::Duration = std::time::Duration::from_secs(30);

/// Caps [`RecordBotGasReceiptCost::redrive_attempts`] so a persistently
/// unavailable RPC endpoint, or a `(chain, tx_hash)` pair whose receipt never
/// resolves (e.g. enqueued against the wrong chain, or a block that was
/// reorged out after confirmation), redrives for a bounded window --
/// `MAX_REDRIVE_ATTEMPTS * REDRIVE_DELAY` = 10 minutes -- rather than
/// looping every `REDRIVE_DELAY` forever. Once exhausted, the job dead-letters
/// via a normal `Err` (loudly, at `error!`, both here and via the worker's
/// terminal-failure log) instead of silently dropping the cost fact.
const MAX_REDRIVE_ATTEMPTS: u32 = 20;

/// Shared dependencies for recording bot-paid gas receipt costs.
pub(crate) struct RecordBotGasReceiptCostCtx {
    pub(crate) base_wallet: Arc<dyn Wallet<Provider = RootProvider>>,
    pub(crate) ethereum_wallet: Arc<dyn Wallet<Provider = RootProvider>>,
    pub(crate) chainlink_feed: Address,
    pub(crate) ledger: BotGasCostLedger,
    /// Used to delayed-redrive a transient RPC-shaped outcome (a lagging RPC
    /// node or an outright RPC error, not a genuine failure) instead of
    /// consuming the apalis retry budget. See [`MAX_REDRIVE_ATTEMPTS`].
    pub(crate) job_queue: RecordBotGasReceiptCostJobQueue,
}

/// Why a single recording attempt failed. Every variant propagates as an
/// `Err` from [`Job::perform`] and is retried by the normal apalis retry
/// policy; a `NonBotPayer` payer mismatch or a conflicting record are
/// invariant violations rather than transient conditions, but the job
/// framework has no "do not retry" signal, so they exhaust the same retry
/// budget and dead-letter like any other failure (see ADR 0017). The
/// exception is a `NonBotPayer` mismatch on a `CctpMint` receipt: CCTP V2's
/// `receiveMessage` is permissionless, so that specific combination is
/// skipped inside `perform` rather than surfacing as one of these variants
/// -- a relayer-paid mint is an expected protocol outcome, not an invariant
/// violation.
#[derive(Debug, thiserror::Error)]
pub(crate) enum RecordBotGasReceiptCostError {
    #[error(transparent)]
    Rpc(#[from] RpcError<TransportErrorKind>),
    #[error("invalid block timestamp {timestamp} on {chain:?}")]
    InvalidBlockTimestamp { chain: Chain, timestamp: u64 },
    #[error(transparent)]
    Valuation(#[from] EthUsdValuationError),
    #[error(transparent)]
    Cost(#[from] BotGasCostError),
    #[error(transparent)]
    Record(#[from] Box<SendError<BotGasReceiptCost>>),
    #[error(transparent)]
    Enqueue(#[from] QueuePushError),
    #[error(
        "receipt {tx_hash} on {chain:?} still unresolved after {attempts} redrive attempts; \
         dead-lettering this gas cost fact"
    )]
    RedriveLimitReached {
        chain: Chain,
        tx_hash: TxHash,
        attempts: u32,
    },
    /// The job names a chain this context holds no wallet for. Recording the
    /// cost would need a payer address to check the receipt against, so the
    /// attempt fails rather than attributing the gas to the wrong wallet.
    #[error("no bot wallet is wired for {chain}, so its receipt gas cannot be recorded")]
    UnwiredChain { chain: Chain },
}

impl From<SendError<BotGasReceiptCost>> for RecordBotGasReceiptCostError {
    fn from(error: SendError<BotGasReceiptCost>) -> Self {
        Self::Record(Box::new(error))
    }
}

impl Job<RecordBotGasReceiptCostCtx> for RecordBotGasReceiptCost {
    type Output = ();
    type Error = RecordBotGasReceiptCostError;

    const WORKER_NAME: &'static str = "bot-gas-receipt-cost-worker";
    const PERFORM_TIMEOUT: Option<std::time::Duration> =
        Some(crate::conductor::job::DEFAULT_PERFORM_TIMEOUT);

    #[cfg(any(test, feature = "test-support"))]
    const JOB_KIND: crate::conductor::job::JobKind =
        crate::conductor::job::JobKind::RecordBotGasReceiptCost;

    fn label(&self) -> Label {
        Label::new(format!("{}:{}", self.chain, self.tx_hash))
    }

    async fn perform(&self, ctx: &RecordBotGasReceiptCostCtx) -> Result<Self::Output, Self::Error> {
        let (provider, bot_wallet) = match self.chain {
            Chain::Base => (ctx.base_wallet.provider(), ctx.base_wallet.address()),
            Chain::Ethereum => (
                ctx.ethereum_wallet.provider(),
                ctx.ethereum_wallet.address(),
            ),
            Chain::HyperEvm => {
                return Err(RecordBotGasReceiptCostError::UnwiredChain { chain: self.chain });
            }
        };

        // A confirmed tx should have a receipt; a missing one here, or an RPC
        // error fetching it, means a load-balanced RPC node has not caught up
        // yet (or is transiently unreachable), not a genuine failure --
        // redrive within the bounded budget rather than burn apalis's tiny
        // fixed retry budget in ~7 seconds.
        let receipt = match provider.get_transaction_receipt(self.tx_hash).await {
            Ok(Some(receipt)) => receipt,
            Ok(None) => {
                return self
                    .redrive_or_dead_letter(ctx, "receipt not yet visible", None)
                    .await;
            }
            Err(error) => {
                return self
                    .redrive_or_dead_letter(ctx, "receipt fetch RPC error", Some(&error))
                    .await;
            }
        };

        // SPEC scopes recording to successful transactions ("a transaction
        // that mines but reverts is not recorded"). Every current enqueue
        // site only enqueues after a confirmation helper that already errors
        // on revert, so this should never actually trigger in production --
        // it is a defense-in-depth check at the one place that validates
        // every enqueued receipt, guarding against a future enqueue site
        // that tolerates reverts. Skip rather than dead-letter: a reverted
        // receipt reaching here is an expected outcome of a known revert
        // race (see ADR 0017 and follow-up-candidates.json for the broader
        // "record reverted-tx gas" work), not an invariant violation that
        // needs operator attention.
        if !receipt.status() {
            warn!(
                target: "rebalance",
                chain = ?self.chain,
                tx_hash = %self.tx_hash,
                "Bot-gas receipt cost: skipping a reverted receipt, not recording a cost for it",
            );
            return Ok(());
        }

        // A confirmed receipt should already carry its block number/hash; a
        // missing one here is the same lagging-RPC-node condition as a
        // missing receipt above -- redrive within the bounded budget instead
        // of burning apalis's tiny fixed retry budget.
        let Some(receipt_block_number) = receipt.block_number else {
            return self
                .redrive_or_dead_letter(ctx, "receipt has no block number yet", None)
                .await;
        };

        // Fetch by the receipt's own block HASH, not by number: between the
        // receipt fetch and this call (or across a job retry) a reorg can
        // replace the block at that height, which would silently attach a
        // timestamp from a different block than the one that actually
        // contains this tx. The same hash is forwarded into
        // `read_eth_usd_price` below so the Chainlink reads are pinned at the
        // same reorg-safe anchor as the timestamp, not just the number.
        let Some(receipt_block_hash) = receipt.block_hash else {
            return self
                .redrive_or_dead_letter(ctx, "receipt has no block hash yet", None)
                .await;
        };

        // CCTP V2's `receiveMessage` is permissionless: an adopted mint
        // (crash-recovery scan, or a resume that finds an already-landed
        // mint) can legitimately have been submitted and paid for by a
        // relayer rather than this bot, unlike every other enqueue site
        // (vault deposit/withdraw, wrap/unwrap, USDC wallet transfer, CCTP
        // burn), which the bot always originates. The payer is knowable from
        // the receipt alone (`receipt.from` vs `bot_wallet`), so check it
        // here -- before the block fetch and the Chainlink valuation calls --
        // rather than after both, so a relayer-paid mint skips cleanly
        // without incurring either RPC read or risking their transient-error
        // redrive/dead-letter path for a receipt whose only correct outcome
        // is a silent skip.
        if self.category == BotGasOperationCategory::CctpMint && receipt.from != bot_wallet {
            warn!(
                target: "rebalance",
                chain = ?self.chain,
                tx_hash = %self.tx_hash,
                "Bot-gas receipt cost: skipping a relayer-paid CCTP mint, not \
                 recording a cost for it",
            );
            return Ok(());
        }

        let block = match provider.get_block_by_hash(receipt_block_hash).await {
            Ok(Some(block)) => block,
            Ok(None) => {
                return self
                    .redrive_or_dead_letter(ctx, "block not yet visible", None)
                    .await;
            }
            Err(error) => {
                return self
                    .redrive_or_dead_letter(ctx, "block fetch RPC error", Some(&error))
                    .await;
            }
        };

        let occurred_at = block_timestamp(self.chain, block.header.timestamp)?;

        let eth_usd_price = match read_eth_usd_price(
            ctx.base_wallet.provider(),
            ctx.chainlink_feed,
            self.chain,
            receipt_block_number,
            receipt_block_hash,
            occurred_at,
        )
        .await
        {
            Ok(price) => price,
            Err(error) if is_transient_rpc_error(&error) => {
                return self
                    .redrive_or_dead_letter(ctx, "valuation RPC error", Some(&error))
                    .await;
            }
            Err(error) => return Err(error.into()),
        };

        // A relayer-paid CCTP mint is already skipped by the early payer
        // check above (before the block fetch and Chainlink valuation), so any
        // `NonBotPayer` reaching `from_receipt` here is a genuine invariant
        // violation for every category, including `CctpMint`, and dead-letters
        // like any other error.
        let cost = BotGasReceiptCost::from_receipt(
            &receipt,
            bot_wallet,
            self.chain,
            self.category,
            self.symbol.clone(),
            eth_usd_price,
            occurred_at,
        )?;

        ctx.ledger.record(&cost).await?;

        Ok(())
    }
}

impl RecordBotGasReceiptCost {
    /// Redrives after a transient RPC-shaped condition (missing receipt,
    /// missing-so-far block, or an outright RPC error fetching either), up to
    /// [`MAX_REDRIVE_ATTEMPTS`]. Past that budget, dead-letters loudly via
    /// `Err` -- both an `error!` log here and the worker's own
    /// terminal-failure log -- instead of looping forever or silently
    /// dropping the gas cost fact.
    async fn redrive_or_dead_letter(
        &self,
        ctx: &RecordBotGasReceiptCostCtx,
        stage: &'static str,
        error: Option<&(dyn std::error::Error + Send + Sync)>,
    ) -> Result<(), RecordBotGasReceiptCostError> {
        if self.redrive_attempts >= MAX_REDRIVE_ATTEMPTS {
            error!(
                target: "rebalance",
                chain = ?self.chain,
                tx_hash = %self.tx_hash,
                stage,
                attempts = self.redrive_attempts,
                max = MAX_REDRIVE_ATTEMPTS,
                ?error,
                "Bot-gas receipt cost: redrive budget exhausted; dead-lettering this gas \
                 cost fact"
            );
            return Err(RecordBotGasReceiptCostError::RedriveLimitReached {
                chain: self.chain,
                tx_hash: self.tx_hash,
                attempts: self.redrive_attempts,
            });
        }

        let next_attempts = self.redrive_attempts + 1;
        warn!(
            target: "rebalance",
            chain = ?self.chain,
            tx_hash = %self.tx_hash,
            stage,
            attempts = next_attempts,
            max = MAX_REDRIVE_ATTEMPTS,
            delay = ?REDRIVE_DELAY,
            ?error,
            "Bot-gas receipt cost: transient RPC condition; rescheduling without \
             consuming apalis retry budget"
        );

        let mut job_queue = ctx.job_queue.clone();
        let redriven = Self {
            redrive_attempts: next_attempts,
            ..self.clone()
        };
        job_queue.push_with_delay(redriven, REDRIVE_DELAY).await?;
        Ok(())
    }
}

/// True when `error` is RPC-shaped (a transport/node problem worth folding
/// into the same bounded RPC redrive budget as a missing receipt) rather than
/// a data or validation problem (a genuinely bad feed reading, which should
/// dead-letter through the normal apalis retry budget instead).
fn is_transient_rpc_error(error: &EthUsdValuationError) -> bool {
    match error {
        EthUsdValuationError::Rpc(_) | EthUsdValuationError::Contract(_) => true,
        EthUsdValuationError::NonPositivePrice { .. }
        | EthUsdValuationError::Decimal { .. }
        | EthUsdValuationError::Arithmetic(_)
        | EthUsdValuationError::InvalidUpdatedAt(_)
        | EthUsdValuationError::DecimalsOutOfRange { .. }
        | EthUsdValuationError::NonEthGasToken { .. } => false,
    }
}

fn block_timestamp(
    chain: Chain,
    timestamp_secs: u64,
) -> Result<DateTime<chrono::Utc>, RecordBotGasReceiptCostError> {
    let secs = i64::try_from(timestamp_secs).map_err(|_| {
        RecordBotGasReceiptCostError::InvalidBlockTimestamp {
            chain,
            timestamp: timestamp_secs,
        }
    })?;

    DateTime::from_timestamp(secs, 0).ok_or(RecordBotGasReceiptCostError::InvalidBlockTimestamp {
        chain,
        timestamp: timestamp_secs,
    })
}

#[cfg(test)]
mod tests {
    use alloy::consensus::{Receipt, ReceiptEnvelope, ReceiptWithBloom};
    use alloy::primitives::{Address, B256, Bloom, Bytes, I256, U256, address};
    use alloy::providers::mock::Asserter;
    use alloy::rpc::client::RpcClient;
    use alloy::rpc::types::{Block, BlockTransactions, Header, TransactionReceipt};
    use alloy::sol_types::SolCall;
    use async_trait::async_trait;
    use chrono::{TimeZone, Utc};

    use st0x_event_sorcery::{Store, StoreBuilder};
    use st0x_evm::{Evm, EvmError};
    use st0x_finance::Symbol;
    use st0x_float_macro::float;

    use super::*;
    use crate::bot_gas::valuation::AggregatorV3Interface;
    use crate::dashboard::pnl::{PnlQuery, build_pnl_report};
    use crate::test_utils::setup_test_pools;

    const CHAINLINK_FEED: Address = address!("0x71041dddad3595F9CEd3DcCFBe3D1F4b0a16Bb70");
    const BOT_WALLET: Address = address!("0x1111111111111111111111111111111111111111");

    struct MockWallet {
        address: Address,
        provider: RootProvider,
    }

    impl MockWallet {
        fn with_asserter(asserter: &Asserter) -> Arc<dyn Wallet<Provider = RootProvider>> {
            Arc::new(Self {
                address: BOT_WALLET,
                provider: RootProvider::new(RpcClient::mocked(asserter.clone())),
            })
        }
    }

    #[async_trait]
    impl Evm for MockWallet {
        type Provider = RootProvider;

        fn provider(&self) -> &RootProvider {
            &self.provider
        }
    }

    #[async_trait]
    impl Wallet for MockWallet {
        fn address(&self) -> Address {
            self.address
        }

        async fn sign_typed_data(
            &self,
            _payload_json: String,
            _expected_digest: alloy::primitives::B256,
        ) -> Result<alloy::primitives::Signature, EvmError> {
            panic!("MockWallet::sign_typed_data should not be called in job tests")
        }

        async fn send_pending(
            &self,
            _contract: Address,
            _calldata: alloy::primitives::Bytes,
            _note: &str,
        ) -> Result<TxHash, EvmError> {
            panic!("MockWallet::send_pending should not be called in job tests")
        }

        async fn await_receipt(&self, _tx_hash: TxHash) -> Result<TransactionReceipt, EvmError> {
            panic!("MockWallet::await_receipt should not be called in job tests")
        }

        async fn send(
            &self,
            _contract: Address,
            _calldata: alloy::primitives::Bytes,
            _note: &str,
        ) -> Result<TransactionReceipt, EvmError> {
            panic!("MockWallet::send should not be called in job tests")
        }
    }

    /// Fixed block hash used by every fixture receipt so `get_block_by_hash`
    /// has a stable hash to match against the mocked block response.
    const FIXTURE_BLOCK_HASH: B256 = B256::repeat_byte(0x22);

    fn receipt(from: Address, block_number: Option<u64>) -> TransactionReceipt {
        TransactionReceipt {
            inner: ReceiptEnvelope::Eip1559(ReceiptWithBloom {
                receipt: Receipt {
                    status: true.into(),
                    cumulative_gas_used: 0,
                    logs: vec![],
                },
                logs_bloom: Bloom::default(),
            }),
            transaction_hash: TxHash::repeat_byte(0x11),
            transaction_index: Some(0),
            block_hash: Some(FIXTURE_BLOCK_HASH),
            block_number,
            to: Some(Address::ZERO),
            contract_address: None,
            from,
            gas_used: 21_000,
            effective_gas_price: 1_000_000_000,
            blob_gas_used: None,
            blob_gas_price: None,
        }
    }

    /// A mined-but-reverted receipt: same shape as `receipt` except
    /// `status: false`.
    fn reverted_receipt(from: Address, block_number: Option<u64>) -> TransactionReceipt {
        let ReceiptEnvelope::Eip1559(ReceiptWithBloom {
            receipt: inner_receipt,
            logs_bloom,
        }) = receipt(from, block_number).inner
        else {
            unreachable!("receipt() always builds an Eip1559 envelope");
        };
        TransactionReceipt {
            inner: ReceiptEnvelope::Eip1559(ReceiptWithBloom {
                receipt: Receipt {
                    status: false.into(),
                    ..inner_receipt
                },
                logs_bloom,
            }),
            ..receipt(from, block_number)
        }
    }

    fn block(timestamp: u64) -> Block {
        Block {
            header: Header::new(alloy::consensus::Header {
                timestamp,
                ..Default::default()
            }),
            uncles: vec![],
            transactions: BlockTransactions::default(),
            withdrawals: None,
        }
    }

    fn encode_decimals_return(decimals: u8) -> alloy::primitives::Bytes {
        alloy::primitives::Bytes::from(AggregatorV3Interface::decimalsCall::abi_encode_returns(
            &decimals,
        ))
    }

    fn encode_price_return(answer: I256, occurred_at: DateTime<Utc>) -> alloy::primitives::Bytes {
        alloy::primitives::Bytes::from(
            AggregatorV3Interface::latestRoundDataCall::abi_encode_returns(
                &AggregatorV3Interface::latestRoundDataReturn {
                    roundId: alloy::primitives::aliases::U80::from_limbs([1, 0]),
                    answer,
                    startedAt: U256::from(occurred_at.timestamp()),
                    updatedAt: U256::from(occurred_at.timestamp()),
                    answeredInRound: alloy::primitives::aliases::U80::from_limbs([1, 0]),
                },
            ),
        )
    }

    async fn ledger_and_store() -> (BotGasCostLedger, Arc<Store<BotGasReceiptCost>>) {
        let (ledger, store, _pool) = ledger_store_and_pool().await;
        (ledger, store)
    }

    async fn ledger_store_and_pool() -> (
        BotGasCostLedger,
        Arc<Store<BotGasReceiptCost>>,
        sqlx::SqlitePool,
    ) {
        let (pool, _apalis_pool) = setup_test_pools().await;
        let store = StoreBuilder::<BotGasReceiptCost>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        (BotGasCostLedger::new(store.clone()), store, pool)
    }

    async fn ctx_with_asserter(
        asserter: &Asserter,
        ledger: BotGasCostLedger,
    ) -> RecordBotGasReceiptCostCtx {
        let apalis_pool = crate::test_utils::setup_test_apalis_pool().await;
        ctx_with_asserter_and_queue(
            asserter,
            ledger,
            RecordBotGasReceiptCostJobQueue::new(&apalis_pool),
        )
    }

    fn ctx_with_asserter_and_queue(
        asserter: &Asserter,
        ledger: BotGasCostLedger,
        job_queue: RecordBotGasReceiptCostJobQueue,
    ) -> RecordBotGasReceiptCostCtx {
        RecordBotGasReceiptCostCtx {
            base_wallet: MockWallet::with_asserter(asserter),
            ethereum_wallet: MockWallet::with_asserter(&Asserter::new()),
            chainlink_feed: CHAINLINK_FEED,
            ledger,
            job_queue,
        }
    }

    fn queue_happy_path_responses(asserter: &Asserter, occurred_at: DateTime<Utc>) {
        asserter.push_success(&receipt(BOT_WALLET, Some(123)));
        asserter.push_success(&block(occurred_at.timestamp().cast_unsigned()));
        asserter.push_success(&encode_decimals_return(8));
        asserter.push_success(&encode_price_return(
            I256::try_from(200_000_000_000_i64).unwrap(),
            occurred_at,
        ));
    }

    async fn perform_with_terminal_price_response(
        price_response: Bytes,
    ) -> (RecordBotGasReceiptCostError, i64) {
        let occurred_at = Utc.with_ymd_and_hms(2026, 7, 23, 12, 0, 0).unwrap();
        let asserter = Asserter::new();
        asserter.push_success(&receipt(BOT_WALLET, Some(123)));
        asserter.push_success(&block(occurred_at.timestamp().cast_unsigned()));
        asserter.push_success(&encode_decimals_return(8));
        asserter.push_success(&price_response);
        let (ledger, _store) = ledger_and_store().await;
        let apalis_pool = crate::test_utils::setup_test_apalis_pool().await;
        let job_queue = RecordBotGasReceiptCostJobQueue::new(&apalis_pool);
        let ctx = ctx_with_asserter_and_queue(&asserter, ledger, job_queue);

        let error = RecordBotGasReceiptCost {
            chain: Chain::Base,
            tx_hash: TxHash::repeat_byte(0x11),
            category: BotGasOperationCategory::VaultDeposit,
            symbol: None,
            redrive_attempts: 0,
        }
        .perform(&ctx)
        .await
        .unwrap_err();

        let pending = sqlx_apalis::query_scalar(
            "SELECT COUNT(*) FROM Jobs WHERE job_type = ? AND status = 'Pending'",
        )
        .bind(std::any::type_name::<RecordBotGasReceiptCost>())
        .fetch_one(&apalis_pool)
        .await
        .unwrap();

        (error, pending)
    }

    /// The context carries a Base and an Ethereum wallet only. A job naming a
    /// third chain has no payer address to check the receipt against, so it
    /// must fail rather than fall back to one of the wallets it does hold --
    /// which would attribute the gas to the wrong wallet.
    #[tokio::test]
    async fn a_chain_with_no_wired_wallet_is_refused() {
        let asserter = Asserter::new();
        let (ledger, _store) = ledger_and_store().await;
        let ctx = ctx_with_asserter(&asserter, ledger).await;

        let job = RecordBotGasReceiptCost {
            chain: Chain::HyperEvm,
            tx_hash: TxHash::repeat_byte(0x11),
            category: BotGasOperationCategory::VaultDeposit,
            symbol: None,
            redrive_attempts: 0,
        };

        let error = job.perform(&ctx).await.unwrap_err();

        assert!(
            matches!(
                error,
                RecordBotGasReceiptCostError::UnwiredChain {
                    chain: Chain::HyperEvm
                }
            ),
            "expected UnwiredChain, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn happy_path_records_cost_for_base_receipt() {
        let occurred_at = Utc.with_ymd_and_hms(2026, 7, 23, 12, 0, 0).unwrap();
        let asserter = Asserter::new();
        queue_happy_path_responses(&asserter, occurred_at);
        let (ledger, store) = ledger_and_store().await;
        let ctx = ctx_with_asserter(&asserter, ledger).await;

        let job = RecordBotGasReceiptCost {
            chain: Chain::Base,
            tx_hash: TxHash::repeat_byte(0x11),
            category: BotGasOperationCategory::VaultDeposit,
            symbol: None,
            redrive_attempts: 0,
        };

        job.perform(&ctx).await.unwrap();

        let id = super::super::BotGasReceiptCostId {
            chain: Chain::Base,
            tx_hash: TxHash::repeat_byte(0x11),
        };
        let recorded = store
            .load(&id)
            .await
            .unwrap()
            .expect("cost should be recorded");
        assert_eq!(recorded.gas_used, 21_000);
        assert_eq!(
            recorded.operation_category,
            BotGasOperationCategory::VaultDeposit
        );
        assert_eq!(recorded.occurred_at, occurred_at);
    }

    #[tokio::test]
    async fn non_bot_payer_is_terminal_not_skipped() {
        let occurred_at = Utc.with_ymd_and_hms(2026, 7, 23, 12, 0, 0).unwrap();
        let asserter = Asserter::new();
        asserter.push_success(&receipt(Address::repeat_byte(0x99), Some(123)));
        asserter.push_success(&block(occurred_at.timestamp().cast_unsigned()));
        asserter.push_success(&encode_decimals_return(8));
        asserter.push_success(&encode_price_return(
            I256::try_from(200_000_000_000_i64).unwrap(),
            occurred_at,
        ));
        let (ledger, _store) = ledger_and_store().await;
        let ctx = ctx_with_asserter(&asserter, ledger).await;

        let job = RecordBotGasReceiptCost {
            chain: Chain::Base,
            tx_hash: TxHash::repeat_byte(0x11),
            category: BotGasOperationCategory::VaultDeposit,
            symbol: None,
            redrive_attempts: 0,
        };

        let error = job.perform(&ctx).await.unwrap_err();

        assert!(matches!(
            error,
            RecordBotGasReceiptCostError::Cost(BotGasCostError::NonBotPayer { .. })
        ));
    }

    /// A relayer-paid CCTP mint (CCTP V2's `receiveMessage` is permissionless,
    /// so an adopted mint can legitimately have been submitted by someone
    /// other than this bot) must be skipped, not dead-lettered as an
    /// invariant violation -- unlike a non-bot payer on any other category
    /// (see `non_bot_payer_is_terminal_not_skipped`), which the bot always
    /// originates. Only the receipt is queued: the payer check now runs
    /// right after the receipt fetch, before the block fetch and Chainlink
    /// valuation calls, so none of those RPC reads should happen.
    #[tokio::test]
    async fn relayer_paid_cctp_mint_is_skipped_not_terminal() {
        let asserter = Asserter::new();
        asserter.push_success(&receipt(Address::repeat_byte(0x99), Some(123)));
        let (ledger, store) = ledger_and_store().await;
        let ctx = ctx_with_asserter(&asserter, ledger).await;

        let job = RecordBotGasReceiptCost {
            chain: Chain::Base,
            tx_hash: TxHash::repeat_byte(0x11),
            category: BotGasOperationCategory::CctpMint,
            symbol: None,
            redrive_attempts: 0,
        };

        job.perform(&ctx)
            .await
            .expect("a relayer-paid CCTP mint must be skipped, not fail the job");

        let id = super::super::BotGasReceiptCostId {
            chain: Chain::Base,
            tx_hash: TxHash::repeat_byte(0x11),
        };
        let recorded = store.load(&id).await.unwrap();
        assert!(
            recorded.is_none(),
            "a relayer-paid CCTP mint must not be recorded as a cost, got {recorded:?}"
        );
    }

    /// A mined-but-reverted receipt must be skipped (no cost recorded, no
    /// error), not recorded as a normal cost -- SPEC scopes recording to
    /// successful transactions.
    #[tokio::test]
    async fn reverted_receipt_is_skipped_not_recorded() {
        let asserter = Asserter::new();
        asserter.push_success(&reverted_receipt(BOT_WALLET, Some(123)));
        let (ledger, store) = ledger_and_store().await;
        let ctx = ctx_with_asserter(&asserter, ledger).await;

        let job = RecordBotGasReceiptCost {
            chain: Chain::Base,
            tx_hash: TxHash::repeat_byte(0x11),
            category: BotGasOperationCategory::VaultDeposit,
            symbol: None,
            redrive_attempts: 0,
        };

        job.perform(&ctx)
            .await
            .expect("a reverted receipt must be skipped, not fail the job");

        let id = super::super::BotGasReceiptCostId {
            chain: Chain::Base,
            tx_hash: TxHash::repeat_byte(0x11),
        };
        let recorded = store.load(&id).await.unwrap();
        assert!(
            recorded.is_none(),
            "a reverted receipt must not be recorded as a cost, got {recorded:?}"
        );
    }

    #[tokio::test]
    async fn conflicting_retry_is_terminal_not_skipped() {
        let occurred_at = Utc.with_ymd_and_hms(2026, 7, 23, 12, 0, 0).unwrap();
        let (ledger, _store) = ledger_and_store().await;

        // First attempt records the fact.
        let first_asserter = Asserter::new();
        queue_happy_path_responses(&first_asserter, occurred_at);
        let first_ctx = ctx_with_asserter(&first_asserter, ledger.clone()).await;
        let job = RecordBotGasReceiptCost {
            chain: Chain::Base,
            tx_hash: TxHash::repeat_byte(0x11),
            category: BotGasOperationCategory::VaultDeposit,
            symbol: None,
            redrive_attempts: 0,
        };
        job.perform(&first_ctx).await.unwrap();

        // Second attempt refetches a receipt reporting different gas used --
        // an immutable-fact conflict for the same (chain, tx_hash), which the
        // aggregate must reject. (A differing USD valuation alone is *not*
        // a conflict: see `identical_retry_is_idempotent`.)
        let second_asserter = Asserter::new();
        let mut conflicting_receipt = receipt(BOT_WALLET, Some(123));
        conflicting_receipt.gas_used += 1;
        second_asserter.push_success(&conflicting_receipt);
        second_asserter.push_success(&block(occurred_at.timestamp().cast_unsigned()));
        second_asserter.push_success(&encode_decimals_return(8));
        second_asserter.push_success(&encode_price_return(
            I256::try_from(200_000_000_000_i64).unwrap(),
            occurred_at,
        ));
        let second_ctx = ctx_with_asserter(&second_asserter, ledger).await;

        let error = job.perform(&second_ctx).await.unwrap_err();

        let RecordBotGasReceiptCostError::Record(send_error) = error else {
            panic!("expected Record, got {error:?}");
        };
        assert!(
            matches!(
                *send_error,
                st0x_event_sorcery::AggregateError::UserError(
                    st0x_event_sorcery::LifecycleError::Apply(
                        super::super::BotGasReceiptCostError::ConflictingReceiptCost
                    )
                )
            ),
            "expected a ConflictingReceiptCost domain rejection, got {send_error:?}"
        );
    }

    /// Uses realistic (non-round) gas/price inputs rather than the clean
    /// numbers used elsewhere in this module, so the recomputed `usd_cost`
    /// on the second attempt has more decimal digits than persistence's
    /// 8-decimal round-trip preserves -- exercising the actual production
    /// idempotency path, not just an input that happens to survive it
    /// unchanged.
    #[tokio::test]
    async fn identical_retry_is_idempotent() {
        let occurred_at = Utc.with_ymd_and_hms(2026, 7, 23, 12, 0, 0).unwrap();
        let (ledger, store, pool) = ledger_store_and_pool().await;
        let job = RecordBotGasReceiptCost {
            chain: Chain::Base,
            tx_hash: TxHash::repeat_byte(0x11),
            category: BotGasOperationCategory::VaultDeposit,
            symbol: None,
            redrive_attempts: 0,
        };

        for _ in 0..2 {
            let asserter = Asserter::new();
            let mut realistic_receipt = receipt(BOT_WALLET, Some(123));
            realistic_receipt.gas_used = 150_000;
            realistic_receipt.effective_gas_price = 5_000_257;
            asserter.push_success(&realistic_receipt);
            asserter.push_success(&block(occurred_at.timestamp().cast_unsigned()));
            asserter.push_success(&encode_decimals_return(8));
            asserter.push_success(&encode_price_return(
                I256::try_from(200_012_345_678_i64).unwrap(),
                occurred_at,
            ));
            let ctx = ctx_with_asserter(&asserter, ledger.clone()).await;
            job.perform(&ctx).await.unwrap();
        }

        let id = super::super::BotGasReceiptCostId {
            chain: Chain::Base,
            tx_hash: TxHash::repeat_byte(0x11),
        };
        let recorded = store
            .load(&id)
            .await
            .unwrap()
            .expect("cost must be recorded after two identical attempts");
        assert_eq!(recorded.gas_used, 150_000);

        let recorded_event_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM events \
             WHERE aggregate_type = 'BotGasReceiptCost' \
               AND event_type = 'BotGasReceiptCostEvent::Recorded'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            recorded_event_count, 1,
            "a matching retry must not append a second Recorded event -- that \
             would double-count this receipt's gas in /pnl"
        );
    }

    /// A missing block (RPC node has the receipt but hasn't indexed its block
    /// by hash yet -- the same load-balanced-RPC-lag scenario as a missing
    /// receipt) must redrive within the bounded budget, same as a missing
    /// receipt, rather than dead-lettering after apalis's tiny default retry
    /// budget.
    #[tokio::test]
    async fn missing_block_redrives_without_terminal_error() {
        let asserter = Asserter::new();
        asserter.push_success(&receipt(BOT_WALLET, Some(123)));
        asserter.push_success(&Option::<Block>::None);
        let (ledger, _store) = ledger_and_store().await;
        let apalis_pool = crate::test_utils::setup_test_apalis_pool().await;
        let job_queue = RecordBotGasReceiptCostJobQueue::new(&apalis_pool);
        let ctx = ctx_with_asserter_and_queue(&asserter, ledger, job_queue);

        let job = RecordBotGasReceiptCost {
            chain: Chain::Base,
            tx_hash: TxHash::repeat_byte(0x11),
            category: BotGasOperationCategory::VaultDeposit,
            symbol: None,
            redrive_attempts: 0,
        };

        job.perform(&ctx)
            .await
            .expect("a missing block must not fail the job terminally");

        let payload: Vec<u8> = sqlx_apalis::query_scalar(
            "SELECT job FROM Jobs WHERE job_type = ? AND status = 'Pending'",
        )
        .bind(std::any::type_name::<RecordBotGasReceiptCost>())
        .fetch_one(&apalis_pool)
        .await
        .unwrap();
        let rescheduled: RecordBotGasReceiptCost = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            rescheduled.redrive_attempts, 1,
            "the rescheduled job must carry an incremented redrive counter"
        );
    }

    #[tokio::test]
    async fn missing_receipt_redrives_without_terminal_error() {
        let asserter = Asserter::new();
        asserter.push_success(&Option::<TransactionReceipt>::None);
        let (ledger, _store) = ledger_and_store().await;
        let apalis_pool = crate::test_utils::setup_test_apalis_pool().await;
        let job_queue = RecordBotGasReceiptCostJobQueue::new(&apalis_pool);
        let ctx = ctx_with_asserter_and_queue(&asserter, ledger, job_queue);

        let job = RecordBotGasReceiptCost {
            chain: Chain::Base,
            tx_hash: TxHash::repeat_byte(0x11),
            category: BotGasOperationCategory::VaultDeposit,
            symbol: None,
            redrive_attempts: 0,
        };

        let before = Utc::now().timestamp();
        job.perform(&ctx)
            .await
            .expect("a missing receipt must not fail the job terminally");
        let after = Utc::now().timestamp();

        let (payload, run_at): (Vec<u8>, i64) = sqlx_apalis::query_as(
            "SELECT job, run_at FROM Jobs \
             WHERE job_type = ? AND status = 'Pending'",
        )
        .bind(std::any::type_name::<RecordBotGasReceiptCost>())
        .fetch_one(&apalis_pool)
        .await
        .unwrap();
        let rescheduled: RecordBotGasReceiptCost = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            rescheduled.tx_hash, job.tx_hash,
            "the rescheduled job must target the same receipt"
        );
        assert_eq!(
            rescheduled.redrive_attempts, 1,
            "the rescheduled job must carry an incremented redrive counter"
        );
        assert!(
            run_at >= before + i64::try_from(REDRIVE_DELAY.as_secs()).unwrap() - 5
                && run_at <= after + i64::try_from(REDRIVE_DELAY.as_secs()).unwrap() + 5,
            "redrive must be delayed by ~{REDRIVE_DELAY:?} -- \
             run_at={run_at} before={before} after={after}"
        );
    }

    /// Once [`MAX_REDRIVE_ATTEMPTS`] is already reached, a further transient
    /// RPC-shaped condition must dead-letter via `Err` instead of redriving
    /// forever -- proving the bounded budget actually terminates.
    #[tokio::test]
    async fn missing_receipt_dead_letters_after_redrive_budget_exhausted() {
        let asserter = Asserter::new();
        asserter.push_success(&Option::<TransactionReceipt>::None);
        let (ledger, _store) = ledger_and_store().await;
        let apalis_pool = crate::test_utils::setup_test_apalis_pool().await;
        let job_queue = RecordBotGasReceiptCostJobQueue::new(&apalis_pool);
        let ctx = ctx_with_asserter_and_queue(&asserter, ledger, job_queue);

        let job = RecordBotGasReceiptCost {
            chain: Chain::Base,
            tx_hash: TxHash::repeat_byte(0x11),
            category: BotGasOperationCategory::VaultDeposit,
            symbol: None,
            redrive_attempts: MAX_REDRIVE_ATTEMPTS,
        };

        let error = job.perform(&ctx).await.unwrap_err();

        assert!(matches!(
            error,
            RecordBotGasReceiptCostError::RedriveLimitReached {
                attempts: MAX_REDRIVE_ATTEMPTS,
                ..
            }
        ));

        let pending_count: i64 = sqlx_apalis::query_scalar(
            "SELECT COUNT(*) FROM Jobs WHERE job_type = ? AND status = 'Pending'",
        )
        .bind(std::any::type_name::<RecordBotGasReceiptCost>())
        .fetch_one(&apalis_pool)
        .await
        .unwrap();
        assert_eq!(
            pending_count, 0,
            "an exhausted redrive budget must not push another redrive"
        );
    }

    #[tokio::test]
    async fn receipt_without_block_hash_is_retryable() {
        let asserter = Asserter::new();
        let mut receipt_without_hash = receipt(BOT_WALLET, Some(123));
        receipt_without_hash.block_hash = None;
        asserter.push_success(&receipt_without_hash);
        let (ledger, _store) = ledger_and_store().await;
        let apalis_pool = crate::test_utils::setup_test_apalis_pool().await;
        let job_queue = RecordBotGasReceiptCostJobQueue::new(&apalis_pool);
        let ctx = ctx_with_asserter_and_queue(&asserter, ledger, job_queue);

        let job = RecordBotGasReceiptCost {
            chain: Chain::Base,
            tx_hash: TxHash::repeat_byte(0x11),
            category: BotGasOperationCategory::VaultDeposit,
            symbol: None,
            redrive_attempts: 0,
        };

        job.perform(&ctx)
            .await
            .expect("a receipt without a block hash must not fail the job terminally");

        let payload: Vec<u8> = sqlx_apalis::query_scalar(
            "SELECT job FROM Jobs WHERE job_type = ? AND status = 'Pending'",
        )
        .bind(std::any::type_name::<RecordBotGasReceiptCost>())
        .fetch_one(&apalis_pool)
        .await
        .unwrap();
        let rescheduled: RecordBotGasReceiptCost = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            rescheduled.redrive_attempts, 1,
            "the rescheduled job must carry an incremented redrive counter"
        );
    }

    /// An RPC error fetching the receipt (e.g. a transient node blip) must
    /// redrive within the bounded budget, same as a missing receipt, rather
    /// than dead-lettering after apalis's tiny ~7s default retry budget.
    #[tokio::test]
    async fn rpc_error_fetching_receipt_redrives_without_terminal_error() {
        let asserter = Asserter::new();
        asserter.push_failure_msg("eth_getTransactionReceipt boom");
        let (ledger, _store) = ledger_and_store().await;
        let apalis_pool = crate::test_utils::setup_test_apalis_pool().await;
        let job_queue = RecordBotGasReceiptCostJobQueue::new(&apalis_pool);
        let ctx = ctx_with_asserter_and_queue(&asserter, ledger, job_queue);

        let job = RecordBotGasReceiptCost {
            chain: Chain::Base,
            tx_hash: TxHash::repeat_byte(0x11),
            category: BotGasOperationCategory::VaultDeposit,
            symbol: None,
            redrive_attempts: 0,
        };

        job.perform(&ctx)
            .await
            .expect("a transient RPC error must not fail the job terminally");

        let payload: Vec<u8> = sqlx_apalis::query_scalar(
            "SELECT job FROM Jobs WHERE job_type = ? AND status = 'Pending'",
        )
        .bind(std::any::type_name::<RecordBotGasReceiptCost>())
        .fetch_one(&apalis_pool)
        .await
        .unwrap();
        let rescheduled: RecordBotGasReceiptCost = serde_json::from_slice(&payload).unwrap();
        assert_eq!(rescheduled.redrive_attempts, 1);
    }

    /// `is_transient_rpc_error` decides whether a valuation failure consumes
    /// the redrive window or dead-letters immediately. A data error (a price
    /// reading no real feed produces) must dead-letter: redriving it would
    /// burn the whole budget on an RPC that will keep returning the same
    /// unusable value.
    #[tokio::test]
    async fn unsupported_feed_decimals_dead_letter_without_redrive() {
        let occurred_at = Utc.with_ymd_and_hms(2026, 7, 23, 12, 0, 0).unwrap();
        let asserter = Asserter::new();
        asserter.push_success(&receipt(BOT_WALLET, Some(123)));
        asserter.push_success(&block(occurred_at.timestamp().cast_unsigned()));
        asserter.push_success(&encode_decimals_return(30));
        let (ledger, _store) = ledger_and_store().await;
        let apalis_pool = crate::test_utils::setup_test_apalis_pool().await;
        let job_queue = RecordBotGasReceiptCostJobQueue::new(&apalis_pool);
        let ctx = ctx_with_asserter_and_queue(&asserter, ledger, job_queue);

        let job = RecordBotGasReceiptCost {
            chain: Chain::Base,
            tx_hash: TxHash::repeat_byte(0x11),
            category: BotGasOperationCategory::VaultDeposit,
            symbol: None,
            redrive_attempts: 0,
        };

        let error = job.perform(&ctx).await.unwrap_err();
        assert!(
            matches!(
                error,
                RecordBotGasReceiptCostError::Valuation(EthUsdValuationError::DecimalsOutOfRange {
                    decimals: 30
                })
            ),
            "unsupported decimals must surface as a terminal valuation error, got {error:?}"
        );

        let pending: i64 = sqlx_apalis::query_scalar(
            "SELECT COUNT(*) FROM Jobs WHERE job_type = ? AND status = 'Pending'",
        )
        .bind(std::any::type_name::<RecordBotGasReceiptCost>())
        .fetch_one(&apalis_pool)
        .await
        .unwrap();
        assert_eq!(
            pending, 0,
            "a data error must dead-letter rather than consume the redrive window"
        );
    }

    #[tokio::test]
    async fn non_positive_feed_price_dead_letters_without_redrive() {
        let occurred_at = Utc.with_ymd_and_hms(2026, 7, 23, 12, 0, 0).unwrap();
        let (error, pending) =
            perform_with_terminal_price_response(encode_price_return(I256::ZERO, occurred_at))
                .await;

        assert!(matches!(
            error,
            RecordBotGasReceiptCostError::Valuation(
                EthUsdValuationError::NonPositivePrice { answer }
            ) if answer.is_zero()
        ));
        assert_eq!(
            pending, 0,
            "a non-positive price must not enqueue a redrive"
        );
    }

    #[tokio::test]
    async fn invalid_feed_update_time_dead_letters_without_redrive() {
        let response = Bytes::from(
            AggregatorV3Interface::latestRoundDataCall::abi_encode_returns(
                &AggregatorV3Interface::latestRoundDataReturn {
                    roundId: alloy::primitives::aliases::U80::from_limbs([1, 0]),
                    answer: I256::try_from(200_000_000_000_i64).unwrap(),
                    startedAt: U256::ZERO,
                    updatedAt: U256::ZERO,
                    answeredInRound: alloy::primitives::aliases::U80::from_limbs([1, 0]),
                },
            ),
        );
        let (error, pending) = perform_with_terminal_price_response(response).await;

        assert!(matches!(
            error,
            RecordBotGasReceiptCostError::Valuation(
                EthUsdValuationError::InvalidUpdatedAt(value)
            ) if value.is_zero()
        ));
        assert_eq!(
            pending, 0,
            "an invalid update time must not enqueue a redrive"
        );
    }

    #[test]
    fn arithmetic_valuation_error_is_not_transient() {
        let source = (float!(1) / float!(0)).unwrap_err();
        let error = EthUsdValuationError::Arithmetic(source);

        assert!(!is_transient_rpc_error(&error));
    }

    /// The other side of the same switch: an RPC failure reading the price is
    /// transient, so it redrives instead of dead-lettering the cost fact.
    #[tokio::test]
    async fn rpc_error_reading_price_redrives_without_terminal_error() {
        let occurred_at = Utc.with_ymd_and_hms(2026, 7, 23, 12, 0, 0).unwrap();
        let asserter = Asserter::new();
        asserter.push_success(&receipt(BOT_WALLET, Some(123)));
        asserter.push_success(&block(occurred_at.timestamp().cast_unsigned()));
        asserter.push_failure_msg("eth_call decimals boom");
        let (ledger, _store) = ledger_and_store().await;
        let apalis_pool = crate::test_utils::setup_test_apalis_pool().await;
        let job_queue = RecordBotGasReceiptCostJobQueue::new(&apalis_pool);
        let ctx = ctx_with_asserter_and_queue(&asserter, ledger, job_queue);

        let job = RecordBotGasReceiptCost {
            chain: Chain::Base,
            tx_hash: TxHash::repeat_byte(0x11),
            category: BotGasOperationCategory::VaultDeposit,
            symbol: None,
            redrive_attempts: 0,
        };

        job.perform(&ctx)
            .await
            .expect("a transient valuation RPC error must not fail the job terminally");

        let payload: Vec<u8> = sqlx_apalis::query_scalar(
            "SELECT job FROM Jobs WHERE job_type = ? AND status = 'Pending'",
        )
        .bind(std::any::type_name::<RecordBotGasReceiptCost>())
        .fetch_one(&apalis_pool)
        .await
        .unwrap();
        let rescheduled: RecordBotGasReceiptCost = serde_json::from_slice(&payload).unwrap();
        assert_eq!(rescheduled.redrive_attempts, 1);
    }

    #[tokio::test]
    async fn receipt_without_block_number_is_retryable() {
        let asserter = Asserter::new();
        asserter.push_success(&receipt(BOT_WALLET, None));
        let (ledger, _store) = ledger_and_store().await;
        let apalis_pool = crate::test_utils::setup_test_apalis_pool().await;
        let job_queue = RecordBotGasReceiptCostJobQueue::new(&apalis_pool);
        let ctx = ctx_with_asserter_and_queue(&asserter, ledger, job_queue);

        let job = RecordBotGasReceiptCost {
            chain: Chain::Base,
            tx_hash: TxHash::repeat_byte(0x11),
            category: BotGasOperationCategory::VaultDeposit,
            symbol: None,
            redrive_attempts: 0,
        };

        job.perform(&ctx)
            .await
            .expect("a receipt without a block number must not fail the job terminally");

        let payload: Vec<u8> = sqlx_apalis::query_scalar(
            "SELECT job FROM Jobs WHERE job_type = ? AND status = 'Pending'",
        )
        .bind(std::any::type_name::<RecordBotGasReceiptCost>())
        .fetch_one(&apalis_pool)
        .await
        .unwrap();
        let rescheduled: RecordBotGasReceiptCost = serde_json::from_slice(&payload).unwrap();
        assert_eq!(
            rescheduled.redrive_attempts, 1,
            "the rescheduled job must carry an incremented redrive counter"
        );
    }

    #[tokio::test]
    async fn ethereum_chain_uses_ethereum_wallet_for_receipt_and_base_wallet_for_valuation() {
        let occurred_at = Utc.with_ymd_and_hms(2026, 7, 23, 12, 0, 0).unwrap();

        let base_asserter = Asserter::new();
        base_asserter.push_success(&999u64);
        base_asserter.push_success(&encode_decimals_return(8));
        base_asserter.push_success(&encode_price_return(
            I256::try_from(200_000_000_000_i64).unwrap(),
            occurred_at,
        ));

        let ethereum_asserter = Asserter::new();
        ethereum_asserter.push_success(&receipt(BOT_WALLET, Some(555)));
        ethereum_asserter.push_success(&block(occurred_at.timestamp().cast_unsigned()));

        let (ledger, store) = ledger_and_store().await;
        let apalis_pool = crate::test_utils::setup_test_apalis_pool().await;
        let ctx = RecordBotGasReceiptCostCtx {
            base_wallet: MockWallet::with_asserter(&base_asserter),
            ethereum_wallet: MockWallet::with_asserter(&ethereum_asserter),
            chainlink_feed: CHAINLINK_FEED,
            ledger,
            job_queue: RecordBotGasReceiptCostJobQueue::new(&apalis_pool),
        };

        // `receipt()` always returns `TxHash::repeat_byte(0x11)` (the mocked
        // transport does not inspect the request), so the job's tx_hash must
        // match for the store lookup key below to line up with what
        // `from_receipt` actually persists.
        let job = RecordBotGasReceiptCost {
            chain: Chain::Ethereum,
            tx_hash: TxHash::repeat_byte(0x11),
            category: BotGasOperationCategory::WalletTransfer,
            symbol: None,
            redrive_attempts: 0,
        };

        job.perform(&ctx).await.unwrap();

        let id = super::super::BotGasReceiptCostId {
            chain: Chain::Ethereum,
            tx_hash: TxHash::repeat_byte(0x11),
        };
        let recorded = store
            .load(&id)
            .await
            .unwrap()
            .expect("cost should be recorded");
        assert_eq!(recorded.eth_usd_price_block_number, Some(999));
    }

    /// Acceptance criterion: a cost recorded through the real
    /// worker + real `BotGasCostLedger` on a real (migrated) SQLite pool is
    /// visible in the `/pnl` report with bot-gas coverage `included`, and a
    /// historical `asOfRowid` taken before recording still excludes it.
    #[tokio::test]
    async fn recorded_cost_is_visible_in_pnl_and_historical_as_of_rowid_excludes_it() {
        let occurred_at = Utc.with_ymd_and_hms(2026, 7, 23, 12, 0, 0).unwrap();
        let (pool, _apalis_pool) = setup_test_pools().await;
        let store = StoreBuilder::<BotGasReceiptCost>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let ledger = BotGasCostLedger::new(store);

        let query = PnlQuery::default();
        let report_before_recording = build_pnl_report(&pool, &query, Vec::new(), Utc::now())
            .await
            .unwrap();
        let as_of_rowid_before_recording = report_before_recording.as_of_rowid;

        let asserter = Asserter::new();
        queue_happy_path_responses(&asserter, occurred_at);
        let ctx = ctx_with_asserter(&asserter, ledger).await;
        let job = RecordBotGasReceiptCost {
            chain: Chain::Base,
            tx_hash: TxHash::repeat_byte(0x11),
            category: BotGasOperationCategory::VaultDeposit,
            symbol: Some(Symbol::new("AAPL").unwrap()),
            redrive_attempts: 0,
        };
        job.perform(&ctx).await.unwrap();

        let latest_report = build_pnl_report(&pool, &query, Vec::new(), Utc::now())
            .await
            .unwrap();
        assert!(latest_report.as_of_rowid > as_of_rowid_before_recording);
        assert_eq!(latest_report.costs.bot_gas_usd, "0.042");
        assert_eq!(latest_report.cost_entries.len(), 1);
        assert_eq!(latest_report.cost_entries[0].category, "bot_gas");
        let bot_gas_coverage = latest_report
            .costs
            .coverage
            .iter()
            .find(|row| row.source == "Bot gas")
            .expect("missing Bot gas coverage row");
        assert_eq!(bot_gas_coverage.status, "included");

        let historical_query = PnlQuery {
            as_of_rowid: Some(as_of_rowid_before_recording),
            ..PnlQuery::default()
        };
        let historical_report = build_pnl_report(&pool, &historical_query, Vec::new(), Utc::now())
            .await
            .unwrap();
        assert_eq!(historical_report.as_of_rowid, as_of_rowid_before_recording);
        assert_eq!(historical_report.costs.bot_gas_usd, "0");
        assert!(historical_report.cost_entries.is_empty());
    }
}

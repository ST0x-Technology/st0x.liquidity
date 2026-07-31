//! Bot-paid gas cost ledger.
use alloy::primitives::{Address, TxHash, U256};
use alloy::rpc::types::TransactionReceipt;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use num_decimal::Num;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::str::FromStr;
use std::sync::{Arc, LazyLock};
use tracing::info;

use st0x_event_sorcery::{DomainEvent, EventSourced, Nil, SendError, Store};
use st0x_finance::Symbol;

mod job;
pub(crate) mod redrive;
mod valuation;

pub(crate) use job::{
    BotGasEnqueueFailure, BotGasReceiptCostEnqueuer, RecordBotGasReceiptCost,
    RecordBotGasReceiptCostCtx, RecordBotGasReceiptCostJobQueue,
};

/// Enqueues bot-gas cost recording for a confirmed Base-chain equity
/// wrap/deposit tx. Shared by the wrapped and unwrapped equity recovery
/// aggregates so the payload shape (notably `redrive_attempts: 0` and the
/// per-symbol attribution) cannot drift between them; each caller `?`s the
/// returned failure into its own `BotGasEnqueueFailed` variant.
pub(crate) async fn enqueue_base_equity_cost(
    bot_gas_enqueuer: &BotGasReceiptCostEnqueuer,
    tx_hash: TxHash,
    category: BotGasOperationCategory,
    symbol: &Symbol,
) -> Result<(), BotGasEnqueueFailure> {
    bot_gas_enqueuer
        .enqueue(RecordBotGasReceiptCost {
            chain: BotGasChain::Base,
            tx_hash,
            category,
            symbol: Some(symbol.clone()),
            redrive_attempts: 0,
        })
        .await
        .map_err(|error| BotGasEnqueueFailure::from_queue_push_error(tx_hash, &error))
}

/// Builds a [`BotGasEnqueueFailure`] for tests outside this module that need
/// to construct one (e.g. stubbing a `BotGasEnqueueFailed` domain error).
/// Keeps `QueuePushFailureKind` module-private -- callers only need a
/// well-formed failure value, not the classification enum itself.
#[cfg(test)]
pub(crate) fn test_bot_gas_enqueue_failure(tx_hash: TxHash) -> BotGasEnqueueFailure {
    job::test_support::enqueue_failure(tx_hash)
}

/// Reads back every `Pending` [`RecordBotGasReceiptCost`] job from the apalis
/// `Jobs` table. Shared by every aggregate's convergence tests (equity mint,
/// USDC cross-venue transfer, wrapped/unwrapped equity recovery) that assert
/// on which bot-gas jobs a confirmed tx enqueued.
#[cfg(test)]
pub(crate) async fn pending_bot_gas_jobs(
    apalis_pool: &apalis_sqlite::SqlitePool,
) -> Vec<RecordBotGasReceiptCost> {
    let payloads: Vec<Vec<u8>> =
        sqlx_apalis::query_scalar("SELECT job FROM Jobs WHERE status = 'Pending' AND job_type = ?")
            .bind(std::any::type_name::<RecordBotGasReceiptCost>())
            .fetch_all(apalis_pool)
            .await
            .unwrap();

    payloads
        .iter()
        .map(|payload| serde_json::from_slice(payload).unwrap())
        .collect()
}

/// Built once and reused: constructed via `Num::from` (infallible integer
/// conversion, unlike `Num::from_str`), so re-deriving it on every
/// `native_cost_usd` call is wasted work rather than a fallible parse.
static WEI_PER_ETH: LazyLock<Num> = LazyLock::new(|| Num::from(1_000_000_000_000_000_000_u64));

/// Mirrors `num_decimal::Num`'s private `MAX_PRECISION` -- the crate does not
/// expose its own constant, so this is kept in sync by hand. `serialize_num`
/// persists a `Num` via `Display`, which rounds to this many decimals; a
/// value must still be positive AFTER that rounding to be usable, since a
/// value that rounds to zero would persist as an unusable zero cost.
const PERSISTED_DECIMAL_PRECISION: usize = 8;

#[derive(Debug, thiserror::Error)]
pub(crate) enum BotGasCostError {
    #[error("receipt payer {receipt_from} does not match bot wallet {bot_wallet}")]
    NonBotPayer {
        receipt_from: Address,
        bot_wallet: Address,
    },
    #[error("native gas cost overflow for receipt {tx_hash}")]
    NativeCostOverflow { tx_hash: TxHash },
    #[error("failed to parse decimal {field}")]
    Decimal {
        field: &'static str,
        #[source]
        source: num_decimal::ParseNumError,
    },
    #[error(transparent)]
    InvalidReceiptCost(#[from] BotGasReceiptCostError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum BotGasChain {
    Base,
    Ethereum,
}

impl BotGasChain {
    fn as_str(self) -> &'static str {
        match self {
            Self::Base => "base",
            Self::Ethereum => "ethereum",
        }
    }
}

impl fmt::Display for BotGasChain {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("expected bot gas chain 'base' or 'ethereum'")]
pub(crate) struct ParseBotGasChainError;

impl FromStr for BotGasChain {
    type Err = ParseBotGasChainError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "base" => Ok(Self::Base),
            "ethereum" => Ok(Self::Ethereum),
            _ => Err(ParseBotGasChainError),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum BotGasOperationCategory {
    VaultDeposit,
    VaultWithdraw,
    Wrap,
    Unwrap,
    CctpBurn,
    CctpMint,
    WalletTransfer,
}

impl BotGasOperationCategory {
    fn as_str(self) -> &'static str {
        match self {
            Self::VaultDeposit => "vault_deposit",
            Self::VaultWithdraw => "vault_withdraw",
            Self::Wrap => "wrap",
            Self::Unwrap => "unwrap",
            Self::CctpBurn => "cctp_burn",
            Self::CctpMint => "cctp_mint",
            Self::WalletTransfer => "wallet_transfer",
        }
    }
}

impl fmt::Display for BotGasOperationCategory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct EthUsdPrice {
    pub(crate) price: Num,
    pub(crate) source: String,
    pub(crate) observed_at: DateTime<Utc>,
    pub(crate) block_number: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct BotGasReceiptCost {
    pub(crate) chain: BotGasChain,
    pub(crate) tx_hash: TxHash,
    pub(crate) receipt_from: Address,
    pub(crate) gas_used: u64,
    pub(crate) effective_gas_price_wei: u128,
    pub(crate) native_cost_wei: U256,
    #[serde(serialize_with = "serialize_num", deserialize_with = "deserialize_num")]
    pub(crate) eth_usd_price: Num,
    pub(crate) eth_usd_price_source: String,
    pub(crate) eth_usd_price_at: DateTime<Utc>,
    pub(crate) eth_usd_price_block_number: Option<u64>,
    #[serde(serialize_with = "serialize_num", deserialize_with = "deserialize_num")]
    pub(crate) usd_cost: Num,
    pub(crate) operation_category: BotGasOperationCategory,
    pub(crate) symbol: Option<Symbol>,
    pub(crate) occurred_at: DateTime<Utc>,
}

fn serialize_num<S>(value: &Num, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&value.to_string())
}

fn deserialize_num<'de, D>(deserializer: D) -> Result<Num, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    Num::from_str(&value).map_err(serde::de::Error::custom)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BotGasReceiptCostId {
    pub(crate) chain: BotGasChain,
    pub(crate) tx_hash: TxHash,
}

impl fmt::Display for BotGasReceiptCostId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:{}", self.chain, self.tx_hash)
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ParseBotGasReceiptCostIdError {
    #[error("bot gas receipt cost id is missing the chain/hash delimiter")]
    MissingDelimiter,
    #[error(transparent)]
    Chain(#[from] ParseBotGasChainError),
    #[error(transparent)]
    TransactionHash(#[from] alloy::hex::FromHexError),
}

impl FromStr for BotGasReceiptCostId {
    type Err = ParseBotGasReceiptCostIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (chain, tx_hash) = value
            .split_once(':')
            .ok_or(ParseBotGasReceiptCostIdError::MissingDelimiter)?;

        Ok(Self {
            chain: chain.parse()?,
            tx_hash: tx_hash.parse()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub(crate) enum BotGasReceiptCostError {
    #[error("receipt gas used must be positive")]
    ZeroGasUsed,
    #[error("receipt effective gas price must be positive")]
    ZeroEffectiveGasPrice,
    #[error("receipt native gas cost must be positive")]
    ZeroNativeCost,
    #[error("ETH/USD valuation must be positive")]
    NonPositiveEthUsdPrice,
    #[error("receipt USD cost must be positive")]
    NonPositiveUsdCost,
    #[error("receipt cost conflicts with the immutable fact already recorded")]
    ConflictingReceiptCost,
}

impl BotGasReceiptCost {
    pub(crate) fn from_receipt(
        receipt: &TransactionReceipt,
        bot_wallet: Address,
        chain: BotGasChain,
        operation_category: BotGasOperationCategory,
        symbol: Option<Symbol>,
        eth_usd_price: EthUsdPrice,
        occurred_at: DateTime<Utc>,
    ) -> Result<Self, BotGasCostError> {
        if receipt.from != bot_wallet {
            return Err(BotGasCostError::NonBotPayer {
                receipt_from: receipt.from,
                bot_wallet,
            });
        }
        if receipt.gas_used == 0 {
            return Err(BotGasReceiptCostError::ZeroGasUsed.into());
        }
        if receipt.effective_gas_price == 0 {
            return Err(BotGasReceiptCostError::ZeroEffectiveGasPrice.into());
        }
        if !eth_usd_price.price.is_positive() {
            return Err(BotGasReceiptCostError::NonPositiveEthUsdPrice.into());
        }

        let effective_gas_price_wei = receipt.effective_gas_price;
        let native_cost_wei = U256::from(receipt.gas_used)
            .checked_mul(U256::from(effective_gas_price_wei))
            .ok_or(BotGasCostError::NativeCostOverflow {
                tx_hash: receipt.transaction_hash,
            })?;
        let usd_cost = native_cost_usd(native_cost_wei, &eth_usd_price.price)?;

        let cost = Self {
            chain,
            tx_hash: receipt.transaction_hash,
            receipt_from: receipt.from,
            gas_used: receipt.gas_used,
            effective_gas_price_wei,
            native_cost_wei,
            eth_usd_price: eth_usd_price.price,
            eth_usd_price_source: eth_usd_price.source,
            eth_usd_price_at: eth_usd_price.observed_at,
            eth_usd_price_block_number: eth_usd_price.block_number,
            usd_cost,
            operation_category,
            symbol,
            occurred_at,
        };
        cost.validate()?;

        Ok(cost)
    }

    pub(crate) fn id(&self) -> BotGasReceiptCostId {
        BotGasReceiptCostId {
            chain: self.chain,
            tx_hash: self.tx_hash,
        }
    }

    /// Compares every field EXCEPT the valuation-derived ones (`eth_usd_price`
    /// and everything sourced from it: `eth_usd_price_source`,
    /// `eth_usd_price_at`, `eth_usd_price_block_number`, `usd_cost`), which a
    /// retry can legitimately recompute differently: they round-trip lossily
    /// through persistence (`serialize_num` rounds to
    /// `num_decimal::MAX_PRECISION`), and an Ethereum receipt's valuation is
    /// deliberately pinned to the Base chain head at recording time (ADR
    /// 0017), which can differ between attempts. A repeated `Record` for the
    /// same receipt is therefore idempotent as long as the facts that came
    /// from the receipt itself still agree (see `handle`'s idempotency
    /// comment). Destructured field-by-field (rather than a hand-listed `&&`
    /// chain) so a newly added struct field fails to compile here until
    /// explicitly classified as immutable-and-compared or
    /// valuation-derived-and-ignored.
    fn matches_immutable_receipt_facts(&self, other: &Self) -> bool {
        let Self {
            chain,
            tx_hash,
            receipt_from,
            gas_used,
            effective_gas_price_wei,
            native_cost_wei,
            eth_usd_price: _,
            eth_usd_price_source: _,
            eth_usd_price_at: _,
            eth_usd_price_block_number: _,
            usd_cost: _,
            operation_category,
            symbol,
            occurred_at,
        } = self;
        let Self {
            chain: other_chain,
            tx_hash: other_tx_hash,
            receipt_from: other_receipt_from,
            gas_used: other_gas_used,
            effective_gas_price_wei: other_effective_gas_price_wei,
            native_cost_wei: other_native_cost_wei,
            eth_usd_price: _,
            eth_usd_price_source: _,
            eth_usd_price_at: _,
            eth_usd_price_block_number: _,
            usd_cost: _,
            operation_category: other_operation_category,
            symbol: other_symbol,
            occurred_at: other_occurred_at,
        } = other;

        chain == other_chain
            && tx_hash == other_tx_hash
            && receipt_from == other_receipt_from
            && gas_used == other_gas_used
            && effective_gas_price_wei == other_effective_gas_price_wei
            && native_cost_wei == other_native_cost_wei
            && operation_category == other_operation_category
            && symbol == other_symbol
            && occurred_at == other_occurred_at
    }

    pub(crate) fn validate(&self) -> Result<(), BotGasReceiptCostError> {
        if self.gas_used == 0 {
            return Err(BotGasReceiptCostError::ZeroGasUsed);
        }
        if self.effective_gas_price_wei == 0 {
            return Err(BotGasReceiptCostError::ZeroEffectiveGasPrice);
        }
        if self.native_cost_wei.is_zero() {
            return Err(BotGasReceiptCostError::ZeroNativeCost);
        }
        if !self.eth_usd_price.is_positive() {
            return Err(BotGasReceiptCostError::NonPositiveEthUsdPrice);
        }
        // Validated against the value as it will actually be PERSISTED
        // (rounded to `PERSISTED_DECIMAL_PRECISION`), not the full-precision
        // value computed here: a sufficiently small positive cost can round
        // down to exactly zero once persisted even though it is positive at
        // full precision, which would silently write an unusable zero cost.
        if !self
            .usd_cost
            .round_with(PERSISTED_DECIMAL_PRECISION)
            .is_positive()
        {
            return Err(BotGasReceiptCostError::NonPositiveUsdCost);
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum BotGasReceiptCostCommand {
    Record { cost: BotGasReceiptCost },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum BotGasReceiptCostEvent {
    Recorded { cost: BotGasReceiptCost },
}

impl DomainEvent for BotGasReceiptCostEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Recorded { .. } => "BotGasReceiptCostEvent::Recorded".to_owned(),
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_owned()
    }
}

#[async_trait]
impl EventSourced for BotGasReceiptCost {
    type Id = BotGasReceiptCostId;
    type Event = BotGasReceiptCostEvent;
    type Command = BotGasReceiptCostCommand;
    type Error = BotGasReceiptCostError;
    type Services = ();
    type Materialized = Nil;

    const AGGREGATE_TYPE: &'static str = "BotGasReceiptCost";
    const PROJECTION: Nil = Nil;
    const SCHEMA_VERSION: u64 = 1;

    fn originate(event: &Self::Event) -> Option<Self> {
        match event {
            BotGasReceiptCostEvent::Recorded { cost } => Some(cost.clone()),
        }
    }

    fn evolve(_entity: &Self, _event: &Self::Event) -> Result<Option<Self>, Self::Error> {
        Ok(None)
    }

    async fn initialize(
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            BotGasReceiptCostCommand::Record { cost } => {
                cost.validate()?;
                Ok(vec![BotGasReceiptCostEvent::Recorded { cost }])
            }
        }
    }

    async fn transition(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            BotGasReceiptCostCommand::Record { cost }
                if self.matches_immutable_receipt_facts(&cost) =>
            {
                if self != &cost {
                    info!(
                        chain = %cost.chain,
                        tx_hash = %cost.tx_hash,
                        "repeated bot gas receipt cost record for the same transaction; \
                         immutable receipt facts match, treating as a no-op despite a \
                         differing USD valuation"
                    );
                }
                Ok(Vec::new())
            }
            BotGasReceiptCostCommand::Record { .. } => {
                Err(BotGasReceiptCostError::ConflictingReceiptCost)
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct BotGasCostLedger {
    store: Arc<Store<BotGasReceiptCost>>,
}

impl BotGasCostLedger {
    pub(crate) fn new(store: Arc<Store<BotGasReceiptCost>>) -> Self {
        Self { store }
    }

    pub async fn record(
        &self,
        cost: &BotGasReceiptCost,
    ) -> Result<(), SendError<BotGasReceiptCost>> {
        self.store
            .send(
                &cost.id(),
                BotGasReceiptCostCommand::Record { cost: cost.clone() },
            )
            .await
    }
}

fn parse_num(field: &'static str, value: &str) -> Result<Num, BotGasCostError> {
    Num::from_str(value).map_err(|source| BotGasCostError::Decimal { field, source })
}

fn native_cost_usd(native_cost_wei: U256, eth_usd_price: &Num) -> Result<Num, BotGasCostError> {
    let native_cost_wei = parse_num("native_cost_wei", &native_cost_wei.to_string())?;

    Ok(&(&native_cost_wei / &*WEI_PER_ETH) * eth_usd_price)
}

#[cfg(test)]
mod tests {
    use alloy::consensus::{Receipt, ReceiptEnvelope, ReceiptWithBloom};
    use alloy::primitives::Bloom;
    use alloy::primitives::{Address, TxHash};
    use alloy::rpc::types::TransactionReceipt;
    use chrono::TimeZone;

    use st0x_event_sorcery::{LifecycleError, TestHarness};

    use super::*;

    fn receipt(from: Address) -> TransactionReceipt {
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
            block_hash: None,
            block_number: Some(123),
            to: Some(Address::ZERO),
            contract_address: None,
            from,
            gas_used: 21_000,
            effective_gas_price: 1_000_000_000,
            blob_gas_used: None,
            blob_gas_price: None,
        }
    }

    fn price() -> EthUsdPrice {
        EthUsdPrice {
            price: Num::from_str("2000").unwrap(),
            source: "eth_usd_valuation_feed".to_owned(),
            observed_at: Utc.with_ymd_and_hms(2026, 7, 16, 12, 0, 0).unwrap(),
            block_number: Some(123),
        }
    }

    #[test]
    fn receipt_cost_values_native_gas_in_usd() {
        let bot = Address::repeat_byte(0x01);
        let cost = BotGasReceiptCost::from_receipt(
            &receipt(bot),
            bot,
            BotGasChain::Base,
            BotGasOperationCategory::VaultDeposit,
            None,
            price(),
            Utc.with_ymd_and_hms(2026, 7, 16, 12, 0, 1).unwrap(),
        )
        .unwrap();

        assert_eq!(cost.native_cost_wei, U256::from(21_000_000_000_000u128));
        assert_eq!(cost.usd_cost.to_string(), "0.042");
    }

    #[test]
    fn receipt_cost_rejects_non_bot_payer() {
        let error = BotGasReceiptCost::from_receipt(
            &receipt(Address::repeat_byte(0x02)),
            Address::repeat_byte(0x01),
            BotGasChain::Base,
            BotGasOperationCategory::VaultDeposit,
            None,
            price(),
            Utc.with_ymd_and_hms(2026, 7, 16, 12, 0, 1).unwrap(),
        )
        .unwrap_err();

        assert!(matches!(error, BotGasCostError::NonBotPayer { .. }));
    }

    #[test]
    fn receipt_cost_rejects_zero_gas_used() {
        let bot = Address::repeat_byte(0x01);
        let mut receipt = receipt(bot);
        receipt.gas_used = 0;

        let error = BotGasReceiptCost::from_receipt(
            &receipt,
            bot,
            BotGasChain::Base,
            BotGasOperationCategory::VaultDeposit,
            None,
            price(),
            Utc.with_ymd_and_hms(2026, 7, 16, 12, 0, 1).unwrap(),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            BotGasCostError::InvalidReceiptCost(BotGasReceiptCostError::ZeroGasUsed)
        ));
    }

    #[test]
    fn receipt_cost_rejects_zero_effective_gas_price() {
        let bot = Address::repeat_byte(0x01);
        let mut receipt = receipt(bot);
        receipt.effective_gas_price = 0;

        let error = BotGasReceiptCost::from_receipt(
            &receipt,
            bot,
            BotGasChain::Base,
            BotGasOperationCategory::VaultDeposit,
            None,
            price(),
            Utc.with_ymd_and_hms(2026, 7, 16, 12, 0, 1).unwrap(),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            BotGasCostError::InvalidReceiptCost(BotGasReceiptCostError::ZeroEffectiveGasPrice)
        ));
    }

    #[test]
    fn receipt_cost_rejects_non_positive_eth_usd_price() {
        let bot = Address::repeat_byte(0x01);
        for value in ["0", "-1"] {
            let mut price = price();
            price.price = Num::from_str(value).unwrap();

            let error = BotGasReceiptCost::from_receipt(
                &receipt(bot),
                bot,
                BotGasChain::Base,
                BotGasOperationCategory::VaultDeposit,
                None,
                price,
                Utc.with_ymd_and_hms(2026, 7, 16, 12, 0, 1).unwrap(),
            )
            .unwrap_err();

            assert!(
                matches!(
                    error,
                    BotGasCostError::InvalidReceiptCost(
                        BotGasReceiptCostError::NonPositiveEthUsdPrice
                    )
                ),
                "ETH/USD price {value} must be rejected"
            );
        }
    }

    /// A `usd_cost` that is positive at full precision but rounds down to
    /// exactly zero once persisted (`serialize_num` rounds to
    /// `PERSISTED_DECIMAL_PRECISION` decimals) must be rejected at record
    /// time, not silently written as an unusable zero cost.
    #[test]
    fn receipt_cost_rejects_usd_cost_that_rounds_to_zero_once_persisted() {
        let bot = Address::repeat_byte(0x01);
        let mut dust_receipt = receipt(bot);
        dust_receipt.gas_used = 1;
        dust_receipt.effective_gas_price = 1;
        let mut dust_price = price();
        // native_cost_wei = 1, so usd_cost = 1e-18 * dust_price -- far below
        // PERSISTED_DECIMAL_PRECISION (8 decimals) but still strictly positive.
        dust_price.price = Num::from_str("0.000001").unwrap();

        let cost = BotGasReceiptCost::from_receipt(
            &dust_receipt,
            bot,
            BotGasChain::Base,
            BotGasOperationCategory::VaultDeposit,
            None,
            dust_price,
            Utc.with_ymd_and_hms(2026, 7, 16, 12, 0, 1).unwrap(),
        );

        let error = cost.expect_err(
            "a usd_cost that rounds to zero once persisted must be rejected at record time",
        );
        assert!(
            matches!(
                error,
                BotGasCostError::InvalidReceiptCost(BotGasReceiptCostError::NonPositiveUsdCost)
            ),
            "expected NonPositiveUsdCost, got: {error:?}"
        );
    }

    /// `validate()` guards against a `usd_cost` that persists as zero by
    /// calling `round_with(PERSISTED_DECIMAL_PRECISION)`, a hand-copied
    /// mirror of `num_decimal::Num`'s private `MAX_PRECISION` (the crate
    /// does not expose it). The actual persisted value instead goes through
    /// `serialize_num`'s `value.to_string()` (`Num`'s `Display`, which
    /// internally rounds via the same `round_with` at its own `MAX_PRECISION`).
    /// This binds the two: for values straddling the 8-decimal boundary,
    /// `round_with(PERSISTED_DECIMAL_PRECISION)` must agree with the actual
    /// serialize-then-reparse round-trip, so a future `num-decimal` version
    /// bump that changes `MAX_PRECISION` fails this test loudly instead of
    /// silently letting `validate()` and persistence disagree.
    #[test]
    fn persisted_decimal_precision_matches_actual_serialization_rounding() {
        for value in [
            "0.000000004999999995",
            "0.000000005000000005",
            "0.000000001",
            "0.0000000049",
            "123.123456785",
        ] {
            let num = Num::from_str(value).unwrap();

            let rounded_via_validate = num.round_with(PERSISTED_DECIMAL_PRECISION);
            let round_tripped_via_persistence = Num::from_str(&num.to_string()).unwrap();

            assert_eq!(
                rounded_via_validate, round_tripped_via_persistence,
                "PERSISTED_DECIMAL_PRECISION ({PERSISTED_DECIMAL_PRECISION}) has drifted from \
                 num_decimal::Num's actual Display/serialization precision for input {value} -- \
                 validate()'s zero-cost guard no longer matches what gets persisted"
            );
        }
    }

    #[tokio::test]
    async fn identical_receipt_cost_retry_is_idempotent() {
        let bot = Address::repeat_byte(0x01);
        let cost = BotGasReceiptCost::from_receipt(
            &receipt(bot),
            bot,
            BotGasChain::Base,
            BotGasOperationCategory::VaultDeposit,
            None,
            price(),
            Utc.with_ymd_and_hms(2026, 7, 16, 12, 0, 1).unwrap(),
        )
        .unwrap();

        TestHarness::<BotGasReceiptCost>::with(())
            .given(vec![BotGasReceiptCostEvent::Recorded {
                cost: cost.clone(),
            }])
            .when(BotGasReceiptCostCommand::Record { cost })
            .await
            .then_expect_events(&[]);
    }

    #[tokio::test]
    async fn conflicting_receipt_cost_retry_is_rejected() {
        let bot = Address::repeat_byte(0x01);
        let cost = BotGasReceiptCost::from_receipt(
            &receipt(bot),
            bot,
            BotGasChain::Base,
            BotGasOperationCategory::VaultDeposit,
            None,
            price(),
            Utc.with_ymd_and_hms(2026, 7, 16, 12, 0, 1).unwrap(),
        )
        .unwrap();
        // Differs in an immutable receipt fact (gas_used), not just the
        // derived USD valuation -- a genuine conflicting fact for the same
        // (chain, tx_hash), which the aggregate must still reject.
        let mut conflicting = cost.clone();
        conflicting.gas_used = cost.gas_used + 1;

        let error = TestHarness::<BotGasReceiptCost>::with(())
            .given(vec![BotGasReceiptCostEvent::Recorded { cost }])
            .when(BotGasReceiptCostCommand::Record { cost: conflicting })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(BotGasReceiptCostError::ConflictingReceiptCost)
        ));
    }

    /// Reproduces the real production idempotency scenario described on
    /// `matches_immutable_receipt_facts`: a retried job's full-precision
    /// valuation disagreeing with the persisted, rounded one must still be
    /// treated as the same fact. Realistic gas/price inputs (not the
    /// contrived clean numbers used elsewhere in this module) produce a
    /// value with more than 8 decimal places, so this exercises the actual
    /// lossy round-trip rather than an input that happens to survive it
    /// unchanged.
    #[tokio::test]
    async fn identical_retry_is_idempotent_despite_lossy_usd_valuation_roundtrip() {
        let bot = Address::repeat_byte(0x01);
        let mut realistic_receipt = receipt(bot);
        realistic_receipt.gas_used = 150_000;
        realistic_receipt.effective_gas_price = 5_000_257;
        let realistic_price = EthUsdPrice {
            price: Num::from_str("2000.12345678").unwrap(),
            source: "eth_usd_valuation_feed".to_owned(),
            observed_at: Utc.with_ymd_and_hms(2026, 7, 16, 12, 0, 0).unwrap(),
            block_number: Some(123),
        };

        let freshly_computed = BotGasReceiptCost::from_receipt(
            &realistic_receipt,
            bot,
            BotGasChain::Base,
            BotGasOperationCategory::VaultDeposit,
            None,
            realistic_price,
            Utc.with_ymd_and_hms(2026, 7, 16, 12, 0, 1).unwrap(),
        )
        .unwrap();

        // Simulate persistence's lossy round-trip through `serialize_num`.
        let rehydrated_usd_cost = Num::from_str(&freshly_computed.usd_cost.to_string()).unwrap();
        assert_ne!(
            rehydrated_usd_cost, freshly_computed.usd_cost,
            "test fixture must exercise the lossy round-trip; adjust the gas/price inputs \
             if this assertion starts failing"
        );
        let mut rehydrated = freshly_computed.clone();
        rehydrated.usd_cost = rehydrated_usd_cost;

        TestHarness::<BotGasReceiptCost>::with(())
            .given(vec![BotGasReceiptCostEvent::Recorded { cost: rehydrated }])
            .when(BotGasReceiptCostCommand::Record {
                cost: freshly_computed,
            })
            .await
            .then_expect_events(&[]);
    }

    #[tokio::test]
    async fn direct_recording_rejects_non_positive_receipt_cost() {
        let bot = Address::repeat_byte(0x01);
        let mut cost = BotGasReceiptCost::from_receipt(
            &receipt(bot),
            bot,
            BotGasChain::Base,
            BotGasOperationCategory::VaultDeposit,
            None,
            price(),
            Utc.with_ymd_and_hms(2026, 7, 16, 12, 0, 1).unwrap(),
        )
        .unwrap();
        cost.usd_cost = Num::from_str("0").unwrap();

        let error = TestHarness::<BotGasReceiptCost>::with(())
            .given_no_previous_events()
            .when(BotGasReceiptCostCommand::Record { cost })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(BotGasReceiptCostError::NonPositiveUsdCost)
        ));
    }
}

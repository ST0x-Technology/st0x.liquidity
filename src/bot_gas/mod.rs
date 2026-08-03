//! Bot-paid gas cost ledger.
use alloy::primitives::{Address, TxHash, U256};
use alloy::rpc::types::TransactionReceipt;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rain_math_float::Float;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use tracing::info;

use st0x_event_sorcery::{DomainEvent, EventSourced, Nil, SendError, Store};
use st0x_finance::{HasZero, Symbol, Usd};
use st0x_float_macro::float;

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

/// Wei per ETH, resolved at compile time so the divisor costs no fallible
/// parse on every `native_cost_usd` call.
const WEI_PER_ETH: Float = float!(1000000000000000000);

/// Decimal places a persisted USD cost is rounded to.
///
/// A value must still be positive AFTER that rounding to be usable, since a
/// value that rounds to zero would persist as an unusable zero cost.
const PERSISTED_DECIMAL_PRECISION: u8 = 8;

/// Half of the smallest unit at `PERSISTED_DECIMAL_PRECISION` (`0.5 * 1e-8`).
const PERSISTED_PRECISION_HALF_UNIT: Float = float!(0.000000005);

/// Smallest unit at `PERSISTED_DECIMAL_PRECISION` (`1e-8`).
const PERSISTED_PRECISION_UNIT: Float = float!(0.00000001);

/// Rounds a USD cost to the precision it is persisted at, using
/// round-half-to-even to preserve the legacy persistence contract.
///
/// `Float::to_fixed_decimal_lossy` truncates toward zero rather than
/// rounding (see docs/float.md). Every caller passes a non-negative cost, so
/// the truncated fixed-point value supplies both the lower neighbor and the
/// retained digit used to break an exact tie. The result is exactly
/// representable at `PERSISTED_DECIMAL_PRECISION` decimals.
fn round_to_persisted_precision(value: Float) -> Result<Float, rain_math_float::FloatError> {
    let (fixed, lossless) = value.to_fixed_decimal_lossy(PERSISTED_DECIMAL_PRECISION)?;
    let truncated = Float::from_fixed_decimal(fixed, PERSISTED_DECIMAL_PRECISION)?;
    if lossless {
        return Ok(truncated);
    }

    let remainder = (value - truncated)?;
    let above_half = remainder.gt(PERSISTED_PRECISION_HALF_UNIT)?;
    let exact_tie = remainder.eq(PERSISTED_PRECISION_HALF_UNIT)?;
    let retained_digit_is_odd = fixed % U256::from(2) == U256::from(1);

    if above_half || (exact_tie && retained_digit_is_odd) {
        truncated + PERSISTED_PRECISION_UNIT
    } else {
        Ok(truncated)
    }
}

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
        source: rain_math_float::FloatError,
    },
    #[error("bot gas arithmetic failed: {0}")]
    Arithmetic(#[from] rain_math_float::FloatError),
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
    pub(crate) price: Usd,
    pub(crate) source: String,
    pub(crate) observed_at: DateTime<Utc>,
    pub(crate) block_number: Option<u64>,
}

/// `Usd` provides its own `Serialize`/`Deserialize` (decimal-string, see
/// docs/float.md) and a hand-written `PartialEq`/`Eq` routed through
/// `Float`'s fallible comparison, so every field on this struct -- including
/// `eth_usd_price` and `usd_cost` -- derives both directly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct BotGasReceiptCost {
    pub(crate) chain: BotGasChain,
    pub(crate) tx_hash: TxHash,
    pub(crate) receipt_from: Address,
    pub(crate) gas_used: u64,
    pub(crate) effective_gas_price_wei: u128,
    pub(crate) native_cost_wei: U256,
    pub(crate) eth_usd_price: Usd,
    pub(crate) eth_usd_price_source: String,
    pub(crate) eth_usd_price_at: DateTime<Utc>,
    pub(crate) eth_usd_price_block_number: Option<u64>,
    pub(crate) usd_cost: Usd,
    pub(crate) operation_category: BotGasOperationCategory,
    pub(crate) symbol: Option<Symbol>,
    pub(crate) occurred_at: DateTime<Utc>,
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

fn validate_positive_comparison(
    comparison: &Result<bool, rain_math_float::FloatError>,
    non_positive: BotGasReceiptCostError,
    comparison_failed: BotGasReceiptCostError,
) -> Result<(), BotGasReceiptCostError> {
    match comparison {
        Ok(true) => Ok(()),
        Ok(false) => Err(non_positive),
        Err(_) => Err(comparison_failed),
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
    #[error("failed to compare ETH/USD valuation with zero")]
    EthUsdPriceComparisonFailed,
    #[error("receipt USD cost must be positive")]
    NonPositiveUsdCost,
    #[error("failed to compare receipt USD cost with zero")]
    UsdCostComparisonFailed,
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
        validate_positive_comparison(
            &eth_usd_price.price.gt(&Usd::ZERO),
            BotGasReceiptCostError::NonPositiveEthUsdPrice,
            BotGasReceiptCostError::EthUsdPriceComparisonFailed,
        )?;

        let effective_gas_price_wei = receipt.effective_gas_price;
        let native_cost_wei = U256::from(receipt.gas_used)
            .checked_mul(U256::from(effective_gas_price_wei))
            .ok_or(BotGasCostError::NativeCostOverflow {
                tx_hash: receipt.transaction_hash,
            })?;
        let usd_cost = native_cost_usd(native_cost_wei, eth_usd_price.price)?;

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
    /// through persistence (rounded to `PERSISTED_DECIMAL_PRECISION` before
    /// it is written), and an Ethereum receipt's valuation is
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
        validate_positive_comparison(
            &self.eth_usd_price.gt(&Usd::ZERO),
            BotGasReceiptCostError::NonPositiveEthUsdPrice,
            BotGasReceiptCostError::EthUsdPriceComparisonFailed,
        )?;
        // `usd_cost` is rounded to `PERSISTED_DECIMAL_PRECISION` when it is
        // built, so the stored value already IS the value that gets
        // persisted. A sufficiently small positive cost rounds to exactly
        // zero there, and this rejects it rather than writing an unusable
        // zero cost.
        validate_positive_comparison(
            &self.usd_cost.gt(&Usd::ZERO),
            BotGasReceiptCostError::NonPositiveUsdCost,
            BotGasReceiptCostError::UsdCostComparisonFailed,
        )?;

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

fn parse_float(field: &'static str, value: &str) -> Result<Float, BotGasCostError> {
    Float::parse(value.to_owned()).map_err(|source| BotGasCostError::Decimal { field, source })
}

fn native_cost_usd(native_cost_wei: U256, eth_usd_price: Usd) -> Result<Usd, BotGasCostError> {
    let native_cost_wei = parse_float("native_cost_wei", &native_cost_wei.to_string())?;
    let cost = ((native_cost_wei / WEI_PER_ETH)? * eth_usd_price.inner())?;

    round_to_persisted_precision(cost)
        .map(Usd::new)
        .map_err(BotGasCostError::Arithmetic)
}

#[cfg(test)]
mod tests {
    use alloy::consensus::{Receipt, ReceiptEnvelope, ReceiptWithBloom};
    use alloy::primitives::Bloom;
    use alloy::primitives::{Address, TxHash};
    use alloy::rpc::types::TransactionReceipt;
    use chrono::TimeZone;
    use serde_json::json;

    use st0x_event_sorcery::{LifecycleError, TestHarness};
    use st0x_float_serde::format_float;

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
            price: Usd::new(float!(2000)),
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
        assert_eq!(format_float(&cost.usd_cost.inner()).unwrap(), "0.042");
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
            price.price = Usd::new(Float::parse(value.to_owned()).unwrap());

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

    #[test]
    fn receipt_cost_preserves_float_comparison_failures() {
        let comparison = Err(rain_math_float::FloatError::InvalidHex(
            "comparison failure fixture".to_owned(),
        ));

        assert_eq!(
            validate_positive_comparison(
                &comparison,
                BotGasReceiptCostError::NonPositiveEthUsdPrice,
                BotGasReceiptCostError::EthUsdPriceComparisonFailed,
            ),
            Err(BotGasReceiptCostError::EthUsdPriceComparisonFailed)
        );

        assert_eq!(
            validate_positive_comparison(
                &Err(rain_math_float::FloatError::InvalidHex(
                    "comparison failure fixture".to_owned(),
                )),
                BotGasReceiptCostError::NonPositiveUsdCost,
                BotGasReceiptCostError::UsdCostComparisonFailed,
            ),
            Err(BotGasReceiptCostError::UsdCostComparisonFailed)
        );
    }

    /// A `usd_cost` that is positive at full precision but rounds down to
    /// exactly zero once persisted (`round_to_persisted_precision` rounds to
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
        dust_price.price = Usd::new(float!(0.000001));

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

    /// `validate()` must check the same rounded value that the event store can
    /// later deserialize.
    #[test]
    fn rounded_usd_cost_survives_the_persistence_roundtrip() {
        let bot = Address::repeat_byte(0x01);

        for value in [
            "0.000000004999999995",
            "0.000000005000000005",
            "0.000000001",
            "0.0000000049",
            "123.123456785",
        ] {
            let rounded =
                round_to_persisted_precision(Float::parse(value.to_owned()).unwrap()).unwrap();
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
            cost.usd_cost = Usd::new(rounded);

            let persisted = serde_json::to_vec(&BotGasReceiptCostEvent::Recorded { cost }).unwrap();
            let rehydrated: BotGasReceiptCostEvent = serde_json::from_slice(&persisted).unwrap();
            let BotGasReceiptCostEvent::Recorded { cost: rehydrated } = rehydrated;

            assert!(
                rounded.eq(rehydrated.usd_cost.inner()).unwrap(),
                "a value rounded to PERSISTED_DECIMAL_PRECISION ({PERSISTED_DECIMAL_PRECISION}) \
                 must survive event serialization unchanged (input {value})"
            );
        }
    }

    /// The unit constants are hand-written literals because `float!` cannot
    /// derive them from `PERSISTED_DECIMAL_PRECISION` at compile time. This
    /// pins all three values together so a precision change cannot silently
    /// break rounding thresholds.
    #[test]
    fn persisted_precision_constants_match_the_declared_precision() {
        let derived_unit = (0..PERSISTED_DECIMAL_PRECISION)
            .try_fold(float!(1), |value, _| value / float!(10))
            .unwrap();
        let derived_half_unit = (derived_unit / float!(2)).unwrap();

        assert!(
            PERSISTED_PRECISION_UNIT.eq(derived_unit).unwrap(),
            "PERSISTED_PRECISION_UNIT must equal 10^-PERSISTED_DECIMAL_PRECISION"
        );
        assert!(
            PERSISTED_PRECISION_HALF_UNIT.eq(derived_half_unit).unwrap(),
            "PERSISTED_PRECISION_HALF_UNIT must be half of PERSISTED_PRECISION_UNIT"
        );
    }

    #[test]
    fn round_to_persisted_precision_preserves_legacy_half_even_rounding() {
        for (value, expected) in [
            ("0.000000004999999995", "0"),
            ("0.000000005000000005", "0.00000001"),
            ("123.12345678499999995", "123.12345678"),
            ("123.123456785", "123.12345678"),
            ("123.12345678500000005", "123.12345679"),
            ("123.123456795", "123.1234568"),
            ("0.999999995", "1"),
            (
                "1234567890123456789012345678901234567890.123456785",
                "1234567890123456789012345678901234567890.12345678",
            ),
        ] {
            let rounded =
                round_to_persisted_precision(Float::parse(value.to_owned()).unwrap()).unwrap();
            let expected = Float::parse(expected.to_owned()).unwrap();

            assert!(
                rounded.eq(expected).unwrap(),
                "input {value} must round half-to-even to {}, got {}",
                format_float(&expected).unwrap(),
                format_float(&rounded).unwrap()
            );
        }
    }

    /// A `native_cost_wei` and `eth_usd_price` that are each individually
    /// valid can still make `native_cost_usd`'s multiplication overflow
    /// `Float`'s 32-bit exponent (see docs/float.md's "Why not
    /// arbitrary-precision rationals" section). That failure must surface as
    /// `BotGasCostError::Arithmetic`, not a panic or a silently wrong cost.
    #[test]
    fn native_cost_usd_arithmetic_overflow_surfaces_as_arithmetic_error() {
        // A sparse (single significant digit) but huge wei amount: `Float`
        // parses it losslessly (unlike `U256::MAX`, whose 78 significant
        // digits exceed `Float`'s ~67-digit coefficient and fail to parse
        // outright), and dividing it by `WEI_PER_ETH` still leaves an
        // exponent large enough that multiplying by `extreme_price` overflows
        // `Float`'s 32-bit exponent.
        let sparse_huge_wei = U256::from(10u64).pow(U256::from(60u64));
        let extreme_price = Usd::new(Float::parse("1e2147483646".to_owned()).unwrap());

        let error = native_cost_usd(sparse_huge_wei, extreme_price).unwrap_err();

        assert!(matches!(error, BotGasCostError::Arithmetic(_)));
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
    /// `matches_immutable_receipt_facts`: a retried job whose valuation
    /// disagrees with the persisted one must still be treated as the same
    /// fact. Since the cost is now rounded to persistence precision when it is
    /// built, the persisted value round-trips exactly and rounding is no
    /// longer a source of disagreement -- but an Ethereum receipt's valuation
    /// is pinned to the Base chain head at recording time (ADR 0017), which
    /// still can differ between attempts. That is the case pinned here.
    #[tokio::test]
    async fn identical_retry_is_idempotent_despite_differing_usd_valuation() {
        let bot = Address::repeat_byte(0x01);
        let mut realistic_receipt = receipt(bot);
        realistic_receipt.gas_used = 150_000;
        realistic_receipt.effective_gas_price = 5_000_257;
        let realistic_price = EthUsdPrice {
            price: Usd::new(float!(2000.12345678)),
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

        // The cost is rounded when it is built, so persistence is lossless:
        // this is the property the explicit rounding buys, and losing it would
        // silently reintroduce the mismatch the exclusion list works around.
        let round_tripped =
            Float::parse(format_float(&freshly_computed.usd_cost.inner()).unwrap()).unwrap();
        assert!(
            round_tripped.eq(freshly_computed.usd_cost.inner()).unwrap(),
            "a persisted usd_cost must survive the serialization round-trip unchanged"
        );

        // A retry can still land on a different valuation (ADR 0017), and the
        // receipt facts alone must decide idempotency.
        let mut rehydrated = freshly_computed.clone();
        rehydrated.usd_cost = (freshly_computed.usd_cost + Usd::new(float!(0.00000001))).unwrap();
        rehydrated.eth_usd_price =
            (freshly_computed.eth_usd_price + Usd::new(float!(0.01))).unwrap();

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
        cost.usd_cost = Usd::ZERO;

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

    /// Legacy `BotGasReceiptCostEvent::Recorded` payloads persisted before
    /// this crate switched `eth_usd_price`/`usd_cost` from
    /// `num_decimal::Num`'s `Display` (a plain decimal string, rounded to 8
    /// dp) to `Float::parse` must still deserialize under the new
    /// deserializer, or aggregate replay of an already-persisted cost fails
    /// closed at boot. Builds a real payload via serde and overwrites the two
    /// numeric fields: `usd_cost` in the reduced-fraction shape `Num`'s
    /// `BigRational`-backed `Display` actually wrote (it reduces to lowest
    /// terms, so it never zero-pads), and `eth_usd_price` deliberately
    /// zero-padded to also cover that shape, which `Num`'s `Display` never
    /// produced but the deserializer must still accept.
    #[test]
    fn deserializes_legacy_num_formatted_persisted_payload() {
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
        let mut payload = serde_json::to_value(&cost).unwrap();
        payload["eth_usd_price"] = json!("3421.87000000");
        payload["usd_cost"] = json!("0.00042135");

        let legacy: BotGasReceiptCost = serde_json::from_value(payload).unwrap();

        assert!(legacy.eth_usd_price.eq(&Usd::new(float!(3421.87))).unwrap());
        assert!(legacy.usd_cost.eq(&Usd::new(float!(0.00042135))).unwrap());
    }

    /// Same legacy-format check for an integer-valued price with no
    /// fractional part, the other shape `Num`'s `Display` produced.
    #[test]
    fn deserializes_legacy_integer_valued_persisted_payload() {
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
        let mut payload = serde_json::to_value(&cost).unwrap();
        payload["eth_usd_price"] = json!("3400");

        let legacy: BotGasReceiptCost = serde_json::from_value(payload).unwrap();

        assert!(legacy.eth_usd_price.eq(&Usd::new(float!(3400))).unwrap());
    }
}

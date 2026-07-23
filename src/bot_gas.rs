//! Bot-paid gas cost ledger.
use alloy::primitives::{Address, TxHash, U256};
use alloy::rpc::types::TransactionReceipt;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use num_decimal::Num;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use st0x_event_sorcery::{DomainEvent, EventSourced, Nil, SendError, Store};
use st0x_finance::Symbol;

const WEI_PER_ETH: &str = "1000000000000000000";

#[derive(Debug, thiserror::Error)]
pub enum BotGasCostError {
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
pub enum BotGasChain {
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
pub struct ParseBotGasChainError;

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
pub enum BotGasOperationCategory {
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
pub struct EthUsdPrice {
    pub price: Num,
    pub source: String,
    pub observed_at: DateTime<Utc>,
    pub block_number: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BotGasReceiptCost {
    pub chain: BotGasChain,
    pub tx_hash: TxHash,
    pub receipt_from: Address,
    pub gas_used: u64,
    pub effective_gas_price_wei: u128,
    pub native_cost_wei: U256,
    #[serde(serialize_with = "serialize_num", deserialize_with = "deserialize_num")]
    pub eth_usd_price: Num,
    pub eth_usd_price_source: String,
    pub eth_usd_price_at: DateTime<Utc>,
    pub eth_usd_price_block_number: Option<u64>,
    #[serde(serialize_with = "serialize_num", deserialize_with = "deserialize_num")]
    pub usd_cost: Num,
    pub operation_category: BotGasOperationCategory,
    pub symbol: Option<Symbol>,
    pub occurred_at: DateTime<Utc>,
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
pub struct BotGasReceiptCostId {
    pub chain: BotGasChain,
    pub tx_hash: TxHash,
}

impl fmt::Display for BotGasReceiptCostId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:{}", self.chain, self.tx_hash)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ParseBotGasReceiptCostIdError {
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
pub enum BotGasReceiptCostError {
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
    pub fn from_receipt(
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

    pub fn id(&self) -> BotGasReceiptCostId {
        BotGasReceiptCostId {
            chain: self.chain,
            tx_hash: self.tx_hash,
        }
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
        if !self.usd_cost.is_positive() {
            return Err(BotGasReceiptCostError::NonPositiveUsdCost);
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BotGasReceiptCostCommand {
    Record { cost: BotGasReceiptCost },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BotGasReceiptCostEvent {
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
            BotGasReceiptCostCommand::Record { cost } if self == &cost => Ok(Vec::new()),
            BotGasReceiptCostCommand::Record { .. } => {
                Err(BotGasReceiptCostError::ConflictingReceiptCost)
            }
        }
    }
}

#[derive(Clone)]
pub struct BotGasCostLedger {
    store: Arc<Store<BotGasReceiptCost>>,
}

impl BotGasCostLedger {
    pub fn new(store: Arc<Store<BotGasReceiptCost>>) -> Self {
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
    let wei_per_eth = parse_num("wei_per_eth", WEI_PER_ETH)?;

    Ok(&(&native_cost_wei / &wei_per_eth) * eth_usd_price)
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

        assert!(
            BotGasReceiptCost::from_receipt(
                &receipt,
                bot,
                BotGasChain::Base,
                BotGasOperationCategory::VaultDeposit,
                None,
                price(),
                Utc.with_ymd_and_hms(2026, 7, 16, 12, 0, 1).unwrap(),
            )
            .is_err()
        );
    }

    #[test]
    fn receipt_cost_rejects_zero_effective_gas_price() {
        let bot = Address::repeat_byte(0x01);
        let mut receipt = receipt(bot);
        receipt.effective_gas_price = 0;

        assert!(
            BotGasReceiptCost::from_receipt(
                &receipt,
                bot,
                BotGasChain::Base,
                BotGasOperationCategory::VaultDeposit,
                None,
                price(),
                Utc.with_ymd_and_hms(2026, 7, 16, 12, 0, 1).unwrap(),
            )
            .is_err()
        );
    }

    #[test]
    fn receipt_cost_rejects_non_positive_eth_usd_price() {
        let bot = Address::repeat_byte(0x01);
        for value in ["0", "-1"] {
            let mut price = price();
            price.price = Num::from_str(value).unwrap();

            assert!(
                BotGasReceiptCost::from_receipt(
                    &receipt(bot),
                    bot,
                    BotGasChain::Base,
                    BotGasOperationCategory::VaultDeposit,
                    None,
                    price,
                    Utc.with_ymd_and_hms(2026, 7, 16, 12, 0, 1).unwrap(),
                )
                .is_err(),
                "ETH/USD price {value} must be rejected"
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
        let mut conflicting = cost.clone();
        conflicting.eth_usd_price = Num::from_str("2100").unwrap();

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

// WIP: RAI-1520

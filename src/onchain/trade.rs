//! Onchain trade conversion and persistence. Converts raw blockchain events
//! ([`RaindexTradeEvent`]) into structured [`OnchainTrade`]s with symbol
//! resolution, price calculation, and Pyth oracle pricing. Also provides
//! vault extraction utilities for the vault registry.

use alloy::primitives::ruint::FromUintError;
use alloy::primitives::{Address, B256, TxHash, U256};
use alloy::providers::Provider;
use alloy::rpc::types::Log;
use alloy::sol_types::SolEvent;
use chrono::{DateTime, Utc};
use rain_math_float::{Float, FloatError};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use st0x_config::EvmCtx;
use st0x_evm::Evm;
use st0x_execution::{Direction, FractionalShares, HasZero, Symbol};
use st0x_float_serde::format_float_with_fallback;

use super::pyth::{extract_pyth_price, raw_price_to_pyth_price};
use crate::bindings::IRaindexV6::{ClearV3, OrderV4, TakeOrderV3};
use crate::onchain::OnChainError;
use crate::onchain::io::{TokenizedSymbol, TradeDetails, Usdc, WrappedTokenizedShares};
use crate::onchain::pyth::PythFeedIds;
use crate::onchain_trade::PythPrice;
use crate::symbol::cache::SymbolCache;

/// Onchain trade event emitted by the Raindex orderbook, wrapping either
/// a `ClearV3` or `TakeOrderV3` payload for downstream processing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaindexTradeEvent {
    ClearV3(Box<ClearV3>),
    TakeOrderV3(Box<TakeOrderV3>),
}

impl RaindexTradeEvent {
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Self::ClearV3(_) => "ClearV3",
            Self::TakeOrderV3(_) => "TakeOrderV3",
        }
    }
}

/// Information about a vault extracted from an order's IO specification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VaultInfo {
    pub(crate) token: Address,
    pub(crate) vault_id: B256,
}

/// Extracts vault information from an OrderV4 for both input and output at the
/// specified indices.
///
/// Returns `(input_vault, output_vault)` if both indices
/// are valid, or `None` if either is out of bounds.
pub(crate) fn extract_vault_info(
    order: &OrderV4,
    input_index: usize,
    output_index: usize,
) -> Option<(VaultInfo, VaultInfo)> {
    let input = order.validInputs.get(input_index)?;
    let output = order.validOutputs.get(output_index)?;

    Some((
        VaultInfo {
            token: input.token,
            vault_id: input.vaultId,
        },
        VaultInfo {
            token: output.token,
            vault_id: output.vaultId,
        },
    ))
}

/// Vault info paired with the order owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OwnedVaultInfo {
    pub(crate) owner: Address,
    pub(crate) vault: VaultInfo,
}

/// Extracts all vault information from a ClearV3 event.
///
/// Returns vaults from both alice and bob orders. Each
/// order contributes two vaults (input and output) as
/// determined by the clear config indices.
pub(crate) fn extract_vaults_from_clear(event: &ClearV3) -> Vec<OwnedVaultInfo> {
    let participants = [
        (
            &event.alice,
            event.clearConfig.aliceInputIOIndex,
            event.clearConfig.aliceOutputIOIndex,
        ),
        (
            &event.bob,
            event.clearConfig.bobInputIOIndex,
            event.clearConfig.bobOutputIOIndex,
        ),
    ];

    participants
        .into_iter()
        .flat_map(|(order, input_idx, output_idx)| {
            extract_owned_vaults(order, input_idx, output_idx)
        })
        .collect()
}

pub(crate) fn extract_owned_vaults(
    order: &OrderV4,
    input_idx: U256,
    output_idx: U256,
) -> Vec<OwnedVaultInfo> {
    let (Ok(in_idx), Ok(out_idx)) = (input_idx.try_into(), output_idx.try_into()) else {
        warn!(
            target: "hedge",
            owner = %order.owner,
            %input_idx,
            %output_idx,
            "extract_owned_vaults: failed to convert input_idx or output_idx from U256 to usize"
        );
        return vec![];
    };

    let Some((input, output)) = extract_vault_info(order, in_idx, out_idx) else {
        warn!(
            target: "hedge",
            owner = %order.owner,
            input_index = %in_idx,
            output_index = %out_idx,
            "extract_vault_info: IO indices out of bounds"
        );
        return vec![];
    };

    vec![
        OwnedVaultInfo {
            owner: order.owner,
            vault: input,
        },
        OwnedVaultInfo {
            owner: order.owner,
            vault: output,
        },
    ]
}

#[derive(Debug, Clone)]
pub struct OnchainTrade {
    pub(crate) tx_hash: TxHash,
    pub(crate) log_index: u64,
    pub(crate) symbol: TokenizedSymbol<WrappedTokenizedShares>,
    pub(crate) equity_token: Address,
    pub(crate) amount: FractionalShares,
    pub(crate) direction: Direction,
    pub(crate) price: Usdc,
    pub(crate) block_timestamp: Option<DateTime<Utc>>,
    pub(crate) gas_used: Option<u64>,
    pub(crate) effective_gas_price: Option<u128>,
    pub(crate) pyth_price: Option<PythPrice>,
}

/// Reads the Pyth reference price for a trade, returning `None` (after a
/// warning) when enrichment cannot proceed. Enrichment is best-effort: a missing
/// feed, an unknown block number (absent from both the log and its receipt), or
/// an RPC failure all skip enrichment without affecting the trade or its hedge.
async fn enrich_with_pyth_price<P: Provider>(
    provider: &P,
    pyth_feed_ids: &PythFeedIds,
    base_symbol: &Symbol,
    block_number: Option<u64>,
    tx_hash: TxHash,
) -> Option<PythPrice> {
    let Some(feed_id) = pyth_feed_ids.get(base_symbol) else {
        // A symbol with no configured feed is an intentional configuration
        // choice, not an anomaly, so this is logged at debug to avoid
        // per-trade warn spam for unconfigured symbols.
        debug!(
            target: "hedge",
            symbol = %base_symbol,
            %tx_hash,
            "No configured Pyth feed for symbol; skipping trade enrichment"
        );
        return None;
    };

    let Some(block_number) = block_number else {
        warn!(
            target: "hedge",
            symbol = %base_symbol,
            %tx_hash,
            "No block number available from log or receipt; skipping Pyth enrichment"
        );
        return None;
    };

    match extract_pyth_price(provider, feed_id, block_number)
        .await
        .and_then(|raw_price| raw_price_to_pyth_price(&raw_price))
    {
        Ok(pyth_price) => Some(pyth_price),
        Err(error) => {
            // `?error` preserves the full source chain (PythError::Rpc boxes
            // the underlying transport error, which Display alone drops).
            warn!(target: "hedge", %tx_hash, ?error, "Pyth price extraction failed");
            None
        }
    }
}

impl OnchainTrade {
    /// Core parsing logic for converting blockchain events to trades
    pub(crate) async fn try_from_order_and_fill_details<EvmImpl: Evm>(
        cache: &SymbolCache,
        evm: &EvmImpl,
        order: OrderV4,
        fill: OrderFill,
        log: Log,
        pyth_feed_ids: &PythFeedIds,
    ) -> Result<Option<Self>, OnChainError> {
        let tx_hash = log.transaction_hash.ok_or(TradeValidationError::NoTxHash)?;
        let log_index = log.log_index.ok_or(TradeValidationError::NoLogIndex)?;

        let receipt = evm.provider().get_transaction_receipt(tx_hash).await?;
        let (gas_used, effective_gas_price, receipt_block_number) = match receipt {
            Some(receipt) => (
                Some(receipt.gas_used),
                Some(receipt.effective_gas_price),
                receipt.block_number,
            ),
            None => (None, None, None),
        };

        let input = order
            .validInputs
            .get(fill.input_index)
            .ok_or(TradeValidationError::NoInputAtIndex(fill.input_index))?;

        let output = order
            .validOutputs
            .get(fill.output_index)
            .ok_or(TradeValidationError::NoOutputAtIndex(fill.output_index))?;

        let onchain_input_amount = Float::from_raw(fill.input_amount);
        let onchain_input_symbol = cache.get_io_symbol(evm, input).await?;

        let onchain_output_amount = Float::from_raw(fill.output_amount);
        let onchain_output_symbol = cache.get_io_symbol(evm, output).await?;

        // Use centralized TradeDetails::try_from_io to extract all trade data consistently
        let trade_details = TradeDetails::try_from_io(
            &onchain_input_symbol,
            onchain_input_amount,
            &onchain_output_symbol,
            onchain_output_amount,
        )?;

        if trade_details.equity_amount().is_zero()? {
            return Ok(None);
        }

        // Calculate price per share in USDC (always USDC amount / equity amount)
        let price_per_share_usdc =
            (trade_details.usdc_amount().value() / trade_details.equity_amount().inner())?;

        if price_per_share_usdc.lt(Float::zero()?)? || price_per_share_usdc.is_zero()? {
            return Ok(None);
        }

        let (equity_symbol_str, equity_token) = if onchain_input_symbol == "USDC" {
            (onchain_output_symbol, output.token)
        } else {
            (onchain_input_symbol, input.token)
        };
        let equity_symbol = TokenizedSymbol::<WrappedTokenizedShares>::parse(&equity_symbol_str)?;

        let pyth_price = enrich_with_pyth_price(
            evm.provider(),
            pyth_feed_ids,
            equity_symbol.base(),
            log.block_number.or(receipt_block_number),
            tx_hash,
        )
        .await;

        let price = Usdc::new(price_per_share_usdc)?;

        let trade = Self {
            tx_hash,
            log_index,
            symbol: equity_symbol,
            equity_token,
            amount: trade_details.equity_amount(),
            direction: trade_details.direction(),
            price,
            block_timestamp: log.block_timestamp.and_then(|timestamp_secs| {
                let secs: i64 = timestamp_secs.try_into().ok()?;
                DateTime::from_timestamp(secs, 0)
            }),
            gas_used,
            effective_gas_price,
            pyth_price,
        };

        Ok(Some(trade))
    }

    /// Attempts to create an OnchainTrade from a transaction hash by looking up
    /// the transaction receipt and parsing relevant orderbook events.
    pub async fn try_from_tx_hash<EvmImpl: Evm>(
        tx_hash: TxHash,
        evm: &EvmImpl,
        cache: &SymbolCache,
        ctx: &EvmCtx,
        pyth_feed_ids: &PythFeedIds,
        order_owner: Address,
    ) -> Result<Option<Self>, OnChainError> {
        let receipt = evm
            .provider()
            .get_transaction_receipt(tx_hash)
            .await?
            .ok_or_else(|| {
                OnChainError::Validation(TradeValidationError::TransactionNotFound(tx_hash))
            })?;

        let trades: Vec<_> = receipt
            .inner
            .logs()
            .iter()
            .filter(|log| {
                (log.topic0() == Some(&ClearV3::SIGNATURE_HASH)
                    || log.topic0() == Some(&TakeOrderV3::SIGNATURE_HASH))
                    && log.address() == ctx.orderbook
            })
            .collect();

        if trades.len() > 1 {
            warn!(
                target: "hedge",
                "Found {} potential trades in the tx with hash {tx_hash}, returning first match",
                trades.len()
            );
        }

        for log in trades {
            if let Some(trade) =
                try_convert_log_to_onchain_trade(log, evm, cache, ctx, pyth_feed_ids, order_owner)
                    .await?
            {
                return Ok(Some(trade));
            }
        }

        Ok(None)
    }
}

#[derive(Debug)]
pub(crate) struct OrderFill {
    pub input_index: usize,
    pub input_amount: B256,
    pub output_index: usize,
    pub output_amount: B256,
}

async fn try_convert_log_to_onchain_trade<EvmImpl: Evm>(
    log: &Log,
    evm: &EvmImpl,
    cache: &SymbolCache,
    ctx: &EvmCtx,
    pyth_feed_ids: &PythFeedIds,
    order_owner: Address,
) -> Result<Option<OnchainTrade>, OnChainError> {
    let log_with_metadata = Log {
        inner: log.inner.clone(),
        block_hash: log.block_hash,
        block_number: log.block_number,
        block_timestamp: log.block_timestamp,
        transaction_hash: log.transaction_hash,
        transaction_index: log.transaction_index,
        log_index: log.log_index,
        removed: false,
    };

    if let Ok(clear_event) = log.log_decode::<ClearV3>() {
        return OnchainTrade::try_from_clear_v3(
            ctx,
            cache,
            evm,
            clear_event.data().clone(),
            log_with_metadata,
            pyth_feed_ids,
            order_owner,
        )
        .await;
    }

    if let Ok(take_order_event) = log.log_decode::<TakeOrderV3>() {
        return OnchainTrade::try_from_take_order_if_target_owner(
            cache,
            evm,
            take_order_event.data().clone(),
            log_with_metadata,
            order_owner,
            pyth_feed_ids,
        )
        .await;
    }

    Ok(None)
}

/// Business logic validation errors for trade processing rules.
#[derive(Debug, thiserror::Error)]
pub(crate) enum TradeValidationError {
    #[error("No transaction hash found in log")]
    NoTxHash,
    #[error("No log index found in log")]
    NoLogIndex,
    #[error("No block number found in log")]
    NoBlockNumber,
    #[error("Integer conversion error: {0}")]
    IntConversion(#[from] std::num::TryFromIntError),
    #[error("Invalid IO index: {0}")]
    InvalidIndex(#[from] FromUintError<usize>),
    #[error("No input found at index: {0}")]
    NoInputAtIndex(usize),
    #[error("No output found at index: {0}")]
    NoOutputAtIndex(usize),
    #[error(
        "Expected IO to contain USDC and one wrapped \
         tokenized equity (wt prefix) but got {0} and {1}"
    )]
    InvalidSymbolConfiguration(String, String),
    #[error("Transaction not found: {0}")]
    TransactionNotFound(TxHash),
    #[error(
        "Node provider issue: tx receipt missing or has no logs. \
        block={block_number}, tx={tx_hash}, clear_log_index={clear_log_index}"
    )]
    NodeReceiptMissing {
        block_number: u64,
        tx_hash: TxHash,
        clear_log_index: u64,
    },
    #[error(
        "Unexpected: tx receipt has ClearV3 but no AfterClearV2 (should be impossible). \
        block={block_number}, tx={tx_hash}, clear_log_index={clear_log_index}"
    )]
    AfterClearMissingFromReceipt {
        block_number: u64,
        tx_hash: TxHash,
        clear_log_index: u64,
    },
    #[error("Negative shares amount: {}", format_float_with_fallback(.0))]
    NegativeShares(Float),
    #[error("Negative USDC amount: {}", format_float_with_fallback(.0))]
    NegativeUsdc(Float),
    #[error("Float error: {0}")]
    Float(#[from] FloatError),
    #[error(
        "symbol '{symbol_provided}' is not a tokenized equity \
         (must have 't' or 'wt' prefix, e.g. tAAPL, wtCOIN)"
    )]
    NotTokenizedEquity { symbol_provided: String },
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use alloy::primitives::{Address, Bytes, U256, address, b256, fixed_bytes, uint};
    use alloy::providers::{ProviderBuilder, mock::Asserter};
    use alloy::sol_types::SolCall;
    use rain_math_float::Float;

    use st0x_evm::ReadOnlyEvm;

    use super::*;
    use crate::bindings::IPyth::getPriceUnsafeCall;
    use crate::bindings::IRaindexV6;
    use crate::bindings::PythStructs::Price;
    use crate::symbol::cache::SymbolCache;
    use st0x_config::EvmCtx;
    use st0x_float_macro::float;

    #[tokio::test]
    async fn enrich_with_pyth_price_returns_price_when_feed_configured() {
        let price = Price {
            price: 15_445_005,
            conf: 21_005,
            expo: -5,
            publishTime: U256::from(1_781_166_017u64),
        };
        let asserter = Asserter::new();
        asserter.push_success(&Bytes::from(getPriceUnsafeCall::abi_encode_returns(&price)));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let symbol = Symbol::new("COIN").unwrap();
        let feed_ids = PythFeedIds::new(HashMap::from([(symbol.clone(), B256::random())]));

        let pyth_price = enrich_with_pyth_price(
            &provider,
            &feed_ids,
            &symbol,
            Some(47_198_127),
            TxHash::random(),
        )
        .await
        .unwrap();

        assert_eq!(pyth_price.value, "15445005");
        assert_eq!(pyth_price.conf, "21005");
        assert_eq!(pyth_price.expo, -5);
        assert_eq!(
            pyth_price.publish_time,
            DateTime::from_timestamp(1_781_166_017, 0).unwrap()
        );
    }

    #[tokio::test]
    async fn enrich_with_pyth_price_skips_when_no_feed_configured() {
        // No responses pushed: an unconfigured feed must not trigger any RPC.
        let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
        let feed_ids = PythFeedIds::default();

        let result = enrich_with_pyth_price(
            &provider,
            &feed_ids,
            &Symbol::new("COIN").unwrap(),
            Some(1),
            TxHash::random(),
        )
        .await;

        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn enrich_with_pyth_price_returns_none_on_invalid_timestamp() {
        // The eth_call succeeds but decodes to an unrepresentable publishTime;
        // raw_price_to_pyth_price rejects it, and enrichment swallows the error.
        let price = Price {
            price: 15_445_005,
            conf: 21_005,
            expo: -5,
            publishTime: U256::MAX,
        };
        let asserter = Asserter::new();
        asserter.push_success(&Bytes::from(getPriceUnsafeCall::abi_encode_returns(&price)));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let symbol = Symbol::new("COIN").unwrap();
        let feed_ids = PythFeedIds::new(HashMap::from([(symbol.clone(), B256::random())]));

        let result = enrich_with_pyth_price(
            &provider,
            &feed_ids,
            &symbol,
            Some(47_198_127),
            TxHash::random(),
        )
        .await;

        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn enrich_with_pyth_price_returns_none_on_rpc_error() {
        // A configured feed and a valid block, but the eth_call fails: enrichment
        // must swallow the error and return None so trade recording is unaffected.
        let asserter = Asserter::new();
        asserter.push_failure_msg("eth_call boom");
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let symbol = Symbol::new("COIN").unwrap();
        let feed_ids = PythFeedIds::new(HashMap::from([(symbol.clone(), B256::random())]));

        let result = enrich_with_pyth_price(
            &provider,
            &feed_ids,
            &symbol,
            Some(47_198_127),
            TxHash::random(),
        )
        .await;

        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn enrich_with_pyth_price_skips_when_block_number_missing() {
        let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
        let symbol = Symbol::new("COIN").unwrap();
        let feed_ids = PythFeedIds::new(HashMap::from([(symbol.clone(), B256::random())]));

        let result =
            enrich_with_pyth_price(&provider, &feed_ids, &symbol, None, TxHash::random()).await;

        assert_eq!(result, None);
    }

    #[test]
    fn test_float_constants_from_v5_interface() {
        // Verify our implementation matches Float constants from LibDecimalFloat.sol

        // FLOAT_ONE = bytes32(uint256(1)) = coefficient=1, exponent=0 -> 1.0
        let float_one = B256::from([
            0x00, 0x00, 0x00, 0x00, // exponent = 0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x01, // coefficient = 1
        ]);
        let diff = (Float::from_raw(float_one) - float!(1.0))
            .unwrap()
            .abs()
            .unwrap();
        assert!(diff.lt(float!(0.000001)).unwrap());

        // FLOAT_HALF = 0xffffffff...05 = coefficient=5, exponent=-1 -> 0.5
        let float_half = B256::from([
            0xff, 0xff, 0xff, 0xff, // exponent = -1
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x05, // coefficient = 5
        ]);
        let diff = (Float::from_raw(float_half) - float!(0.5))
            .unwrap()
            .abs()
            .unwrap();
        assert!(diff.lt(float!(0.000001)).unwrap());

        // FLOAT_TWO = bytes32(uint256(2)) = coefficient=2, exponent=0 -> 2.0
        let float_two = B256::from([
            0x00, 0x00, 0x00, 0x00, // exponent = 0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x02, // coefficient = 2
        ]);
        let diff = (Float::from_raw(float_two) - float!(2.0))
            .unwrap()
            .abs()
            .unwrap();
        assert!(diff.lt(float!(0.000001)).unwrap());
    }

    /// Test with real production event data from tx
    /// 0xf05d...304d (2 shares of tSPLG at ~$80/share).
    ///
    /// TakeOrderV3 event field semantics (counterintuitive naming):
    /// - event.input = amount the order GAVE = 2 tSPLG shares
    /// - event.output = amount the order RECEIVED = ~160 USDC
    #[test]
    fn test_from_raw_production_event_data() {
        // event.input Float: 2 shares the order gave
        // Raw bytes: ffffffee00000000000000000000000000000000000000001bc16d674ec80000
        let event_input_float =
            fixed_bytes!("ffffffee00000000000000000000000000000000000000001bc16d674ec80000");
        let shares_amount = Float::from_raw(event_input_float);
        let diff = (shares_amount - float!(2.0)).unwrap().abs().unwrap();
        assert!(
            diff.lt(float!(0.000001)).unwrap(),
            "Expected 2.0 shares but got {shares_amount:?}"
        );

        // event.output Float: ~160 USDC the order received
        // Raw bytes: ffffffe500000000000000000000000000000002057d2cd516a29b6174400000
        let event_output_float =
            fixed_bytes!("ffffffe500000000000000000000000000000002057d2cd516a29b6174400000");
        let usdc_amount = Float::from_raw(event_output_float);
        let diff = (usdc_amount - float!(160.15507752)).unwrap().abs().unwrap();
        assert!(
            diff.lt(float!(0.00001)).unwrap(),
            "Expected ~160.15 USDC but got {usdc_amount:?}"
        );

        // After swapping (as done in take_order.rs):
        // - input_amount (order's input token = USDC) = event.output = 160.15
        // - output_amount (order's output token = tSPLG) = event.input = 2.0
        // Then TradeDetails::try_from_io("USDC", 160.15, "wtSPLG", 2.0) correctly extracts:
        // - equity_amount = 2.0 (from output since output is tokenized equity)
        // - usdc_amount = 160.15 (from input since input is USDC)
    }

    #[test]
    fn test_from_raw_edge_cases() {
        let float_zero = Float::from_fixed_decimal(uint!(0_U256), 0)
            .unwrap()
            .get_inner();
        let diff = (Float::from_raw(float_zero) - float!(0))
            .unwrap()
            .abs()
            .unwrap();
        assert!(diff.lt(float!(0.000001)).unwrap());

        let float_one = Float::from_fixed_decimal(uint!(1_U256), 0)
            .unwrap()
            .get_inner();
        let diff = (Float::from_raw(float_one) - float!(1))
            .unwrap()
            .abs()
            .unwrap();
        assert!(diff.lt(float!(0.000001)).unwrap());

        let float_nine = Float::from_fixed_decimal(uint!(9_U256), 0)
            .unwrap()
            .get_inner();
        let result = Float::from_raw(float_nine);
        let diff = (result - float!(9)).unwrap().abs().unwrap();
        assert!(diff.lt(float!(0.000001)).unwrap());

        let float_hundred = Float::from_fixed_decimal(uint!(100_U256), 0)
            .unwrap()
            .get_inner();
        let result = Float::from_raw(float_hundred);
        let diff = (result - float!(100)).unwrap().abs().unwrap();
        assert!(diff.lt(float!(0.000001)).unwrap());

        let float_half = Float::from_fixed_decimal(uint!(5_U256), 1)
            .unwrap()
            .get_inner();
        let result = Float::from_raw(float_half);
        let diff = (result - float!(0.5)).unwrap().abs().unwrap();
        assert!(diff.lt(float!(0.000001)).unwrap());
    }

    #[test]
    fn test_from_raw_large_values() {
        // Test with very large coefficient
        let large_coeff = 1_000_000_000_000_000_i128;
        let float_large = Float::from_fixed_decimal_lossy(U256::from(large_coeff), 0)
            .unwrap()
            .0
            .get_inner();
        let result = Float::from_raw(float_large);
        let diff = (result - float!(1000000000000000)).unwrap().abs().unwrap();
        assert!(diff.lt(float!(1)).unwrap());

        // Test with very small value (high negative exponent)
        // Float preserves small values that Decimal truncated to zero
        let float_small = Float::from_fixed_decimal_lossy(uint!(1_U256), 50)
            .unwrap()
            .0
            .get_inner();
        let result = Float::from_raw(float_small);
        assert!(
            !result.is_zero().unwrap(),
            "Expected tiny value to remain non-zero after conversion"
        );
        let diff = result.abs().unwrap();
        assert!(diff.lt(float!(0.000001)).unwrap());
    }

    #[test]
    fn test_from_raw_formatting_edge_cases() {
        let float_amount = Float::from_fixed_decimal(uint!(123_456_U256), 6)
            .unwrap()
            .get_inner();
        let result = Float::from_raw(float_amount);
        let diff = (result - float!(0.123456)).unwrap().abs().unwrap();
        assert!(diff.lt(float!(0.000001)).unwrap());

        let float_amount = Float::from_fixed_decimal(uint!(5_U256), 10)
            .unwrap()
            .get_inner();
        let result = Float::from_raw(float_amount);
        let diff = (result - float!(&"0.0000000005".to_string()))
            .unwrap()
            .abs()
            .unwrap();
        assert!(diff.lt(float!(&"0.000000000000001".to_string())).unwrap());

        let float_amount = Float::from_fixed_decimal(uint!(12_345_U256), 0)
            .unwrap()
            .get_inner();
        let result = Float::from_raw(float_amount);
        let diff = (result - float!(12345)).unwrap().abs().unwrap();
        assert!(diff.lt(float!(0.000001)).unwrap());

        let float_amount = Float::from_fixed_decimal(uint!(5000_U256), 0)
            .unwrap()
            .get_inner();
        let result = Float::from_raw(float_amount);
        let diff = (result - float!(5000)).unwrap().abs().unwrap();
        assert!(diff.lt(float!(0.000001)).unwrap());
    }

    #[tokio::test]
    async fn test_try_from_tx_hash_transaction_not_found() {
        let asserter = Asserter::new();
        // Mock the eth_getTransactionReceipt call to return null (transaction not found)
        asserter.push_success(&serde_json::Value::Null);
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let cache = SymbolCache::default();
        let pyth_feed_ids = PythFeedIds::default();
        let ctx = EvmCtx {
            rpc_url: "http://localhost:8545".parse().unwrap(),
            orderbook: Address::ZERO,
            deployment_block: 0,
            required_confirmations: 0,
        };

        let tx_hash =
            fixed_bytes!("0x4444444444444444444444444444444444444444444444444444444444444444");

        // Mock returns empty response by default, simulating transaction not found
        let result = OnchainTrade::try_from_tx_hash(
            tx_hash,
            &ReadOnlyEvm::new(provider),
            &cache,
            &ctx,
            &pyth_feed_ids,
            Address::ZERO,
        )
        .await;

        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(TradeValidationError::TransactionNotFound(_))
        ));
    }

    fn make_io(token: Address, vault_id: B256) -> IRaindexV6::IOV2 {
        IRaindexV6::IOV2 {
            token,
            vaultId: vault_id,
        }
    }

    fn make_order(
        owner: Address,
        inputs: Vec<IRaindexV6::IOV2>,
        outputs: Vec<IRaindexV6::IOV2>,
    ) -> IRaindexV6::OrderV4 {
        IRaindexV6::OrderV4 {
            owner,
            evaluable: IRaindexV6::EvaluableV4::default(),
            validInputs: inputs,
            validOutputs: outputs,
            nonce: B256::ZERO,
        }
    }

    #[test]
    fn extract_vault_info_returns_none_for_out_of_bounds_input() {
        let order = make_order(
            address!("0x1111111111111111111111111111111111111111"),
            vec![make_io(
                address!("0x2222222222222222222222222222222222222222"),
                b256!("0x0000000000000000000000000000000000000000000000000000000000000001"),
            )],
            vec![make_io(
                address!("0x3333333333333333333333333333333333333333"),
                b256!("0x0000000000000000000000000000000000000000000000000000000000000002"),
            )],
        );

        assert!(extract_vault_info(&order, 1, 0).is_none());
    }

    #[test]
    fn extract_vault_info_returns_none_for_out_of_bounds_output() {
        let order = make_order(
            address!("0x1111111111111111111111111111111111111111"),
            vec![make_io(
                address!("0x2222222222222222222222222222222222222222"),
                b256!("0x0000000000000000000000000000000000000000000000000000000000000001"),
            )],
            vec![make_io(
                address!("0x3333333333333333333333333333333333333333"),
                b256!("0x0000000000000000000000000000000000000000000000000000000000000002"),
            )],
        );

        assert!(extract_vault_info(&order, 0, 1).is_none());
    }

    #[test]
    fn extract_vault_info_extracts_valid_indices() {
        let input_token = address!("0x2222222222222222222222222222222222222222");
        let output_token = address!("0x3333333333333333333333333333333333333333");
        let input_vault =
            b256!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let output_vault =
            b256!("0x0000000000000000000000000000000000000000000000000000000000000002");

        let order = make_order(
            address!("0x1111111111111111111111111111111111111111"),
            vec![make_io(input_token, input_vault)],
            vec![make_io(output_token, output_vault)],
        );

        let (input, output) = extract_vault_info(&order, 0, 0).unwrap();

        assert_eq!(input.token, input_token);
        assert_eq!(input.vault_id, input_vault);
        assert_eq!(output.token, output_token);
        assert_eq!(output.vault_id, output_vault);
    }

    #[test]
    fn extract_vaults_from_clear_extracts_both_orders() {
        let alice_owner = address!("0xaaaa000000000000000000000000000000000001");
        let bob_owner = address!("0xbbbb000000000000000000000000000000000002");

        let alice = make_order(
            alice_owner,
            vec![make_io(
                address!("0x1111111111111111111111111111111111111111"),
                b256!("0x0000000000000000000000000000000000000000000000000000000000000001"),
            )],
            vec![make_io(
                address!("0x2222222222222222222222222222222222222222"),
                b256!("0x0000000000000000000000000000000000000000000000000000000000000002"),
            )],
        );

        let bob = make_order(
            bob_owner,
            vec![make_io(
                address!("0x3333333333333333333333333333333333333333"),
                b256!("0x0000000000000000000000000000000000000000000000000000000000000003"),
            )],
            vec![make_io(
                address!("0x4444444444444444444444444444444444444444"),
                b256!("0x0000000000000000000000000000000000000000000000000000000000000004"),
            )],
        );

        let event = IRaindexV6::ClearV3 {
            sender: address!("0x0000000000000000000000000000000000000000"),
            alice,
            bob,
            clearConfig: IRaindexV6::ClearConfigV2 {
                aliceInputIOIndex: uint!(0_U256),
                aliceOutputIOIndex: uint!(0_U256),
                bobInputIOIndex: uint!(0_U256),
                bobOutputIOIndex: uint!(0_U256),
                aliceBountyVaultId: B256::ZERO,
                bobBountyVaultId: B256::ZERO,
            },
        };

        let vaults = extract_vaults_from_clear(&event);

        assert_eq!(vaults.len(), 4);
        assert_eq!(vaults[0].owner, alice_owner);
        assert_eq!(vaults[1].owner, alice_owner);
        assert_eq!(vaults[2].owner, bob_owner);
        assert_eq!(vaults[3].owner, bob_owner);
    }

    #[test]
    fn extract_owned_vaults_extracts_order_vaults() {
        let owner = address!("0xaaaa000000000000000000000000000000000001");
        let input_token = address!("0x1111111111111111111111111111111111111111");
        let output_token = address!("0x2222222222222222222222222222222222222222");
        let input_vault =
            b256!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let output_vault =
            b256!("0x0000000000000000000000000000000000000000000000000000000000000002");

        let order = make_order(
            owner,
            vec![make_io(input_token, input_vault)],
            vec![make_io(output_token, output_vault)],
        );

        let vaults = extract_owned_vaults(&order, uint!(0_U256), uint!(0_U256));

        assert_eq!(vaults.len(), 2);
        assert_eq!(vaults[0].owner, owner);
        assert_eq!(vaults[0].vault.token, input_token);
        assert_eq!(vaults[0].vault.vault_id, input_vault);
        assert_eq!(vaults[1].owner, owner);
        assert_eq!(vaults[1].vault.token, output_token);
        assert_eq!(vaults[1].vault.vault_id, output_vault);
    }

    #[test]
    fn extract_owned_vaults_returns_empty_for_invalid_indices() {
        let owner = address!("0xaaaa000000000000000000000000000000000001");

        let order = make_order(
            owner,
            vec![make_io(
                address!("0x1111111111111111111111111111111111111111"),
                b256!("0x0000000000000000000000000000000000000000000000000000000000000001"),
            )],
            vec![],
        );

        let vaults = extract_owned_vaults(&order, uint!(0_U256), uint!(0_U256));
        assert!(vaults.is_empty());
    }
}

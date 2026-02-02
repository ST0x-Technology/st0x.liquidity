use alloy::primitives::{Address, B256, U256};
use alloy::providers::Provider;
use alloy::rpc::types::Log;
use alloy::sol_types::SolEvent;
use chrono::{DateTime, Utc};
use rain_math_float::Float;
use serde::{Deserialize, Serialize};
use tracing::{error, warn};

use super::pyth::PythPricing;
use crate::bindings::IOrderBookV5::{ClearV3, OrderV4, TakeOrderV3};
use crate::error::{OnChainError, TradeValidationError};
use crate::onchain::EvmConfig;
use crate::onchain::io::{TokenizedEquitySymbol, TradeDetails, Usdc};
use crate::onchain::pyth::FeedIdCache;
use crate::symbol::cache::SymbolCache;
use st0x_execution::Direction;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TradeEvent {
    ClearV3(Box<ClearV3>),
    TakeOrderV3(Box<TakeOrderV3>),
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
/// Returns `(input_vault, output_vault)` if both indices are valid, or None if either is out of bounds.
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
/// Returns vaults from both alice and bob orders. Each order contributes two vaults
/// (input and output) as determined by the clear config indices.
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
            owner = %order.owner,
            %input_idx,
            %output_idx,
            "extract_owned_vaults: failed to convert input_idx or output_idx from U256 to usize"
        );
        return vec![];
    };

    let Some((input, output)) = extract_vault_info(order, in_idx, out_idx) else {
        warn!(
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

#[derive(Debug, Clone, PartialEq)]
pub struct OnchainTrade {
    pub(crate) id: Option<i64>,
    pub(crate) tx_hash: B256,
    pub(crate) log_index: u64,
    pub(crate) symbol: TokenizedEquitySymbol,
    pub(crate) equity_token: Address,
    pub(crate) amount: f64,
    pub(crate) direction: Direction,
    pub(crate) price: Usdc,
    pub(crate) block_timestamp: Option<DateTime<Utc>>,
    pub(crate) created_at: Option<DateTime<Utc>>,
    pub(crate) gas_used: Option<u64>,
    pub(crate) effective_gas_price: Option<u128>,
    pub(crate) pyth_price: Option<f64>,
    pub(crate) pyth_confidence: Option<f64>,
    pub(crate) pyth_exponent: Option<i32>,
    pub(crate) pyth_publish_time: Option<DateTime<Utc>>,
}

impl OnchainTrade {
    /// Core parsing logic for converting blockchain events to trades
    pub(crate) async fn try_from_order_and_fill_details<P: Provider>(
        cache: &SymbolCache,
        provider: P,
        order: OrderV4,
        fill: OrderFill,
        log: Log,
        feed_id_cache: &FeedIdCache,
    ) -> Result<Option<Self>, OnChainError> {
        let tx_hash = log.transaction_hash.ok_or(TradeValidationError::NoTxHash)?;
        let log_index = log.log_index.ok_or(TradeValidationError::NoLogIndex)?;

        // Fetch transaction receipt to get gas information
        let receipt = provider.get_transaction_receipt(tx_hash).await?;
        let (gas_used, effective_gas_price) = match receipt {
            Some(receipt) => (Some(receipt.gas_used), Some(receipt.effective_gas_price)),
            None => (None, None),
        };

        let input = order
            .validInputs
            .get(fill.input_index)
            .ok_or(TradeValidationError::NoInputAtIndex(fill.input_index))?;

        let output = order
            .validOutputs
            .get(fill.output_index)
            .ok_or(TradeValidationError::NoOutputAtIndex(fill.output_index))?;

        let onchain_input_amount = float_to_f64(fill.input_amount)?;
        let onchain_input_symbol = cache.get_io_symbol(&provider, input).await?;

        let onchain_output_amount = float_to_f64(fill.output_amount)?;
        let onchain_output_symbol = cache.get_io_symbol(&provider, output).await?;

        // Use centralized TradeDetails::try_from_io to extract all trade data consistently
        let trade_details = TradeDetails::try_from_io(
            &onchain_input_symbol,
            onchain_input_amount,
            &onchain_output_symbol,
            onchain_output_amount,
        )?;

        if trade_details.equity_amount().value() == 0.0 {
            return Ok(None);
        }

        // Calculate price per share in USDC (always USDC amount / equity amount)
        let price_per_share_usdc =
            trade_details.usdc_amount().value() / trade_details.equity_amount().value();

        if price_per_share_usdc.is_nan() || price_per_share_usdc <= 0.0 {
            return Ok(None);
        }

        // Parse the tokenized equity symbol to ensure it's valid
        let (tokenized_symbol_str, equity_token) = if onchain_input_symbol == "USDC" {
            (onchain_output_symbol, output.token)
        } else {
            (onchain_input_symbol, input.token)
        };
        let tokenized_symbol = TokenizedEquitySymbol::parse(&tokenized_symbol_str)?;

        let pyth_pricing = match PythPricing::try_from_tx_hash(
            tx_hash,
            &provider,
            &tokenized_symbol.base().to_string(),
            feed_id_cache,
        )
        .await
        {
            Ok(pricing) => Some(pricing),
            Err(e) => {
                error!("Failed to get Pyth pricing for tx_hash={tx_hash:?}: {e}");
                None
            }
        };

        let price = Usdc::new(price_per_share_usdc)?;

        let trade = Self {
            id: None,
            tx_hash,
            log_index,
            symbol: tokenized_symbol,
            equity_token,
            amount: trade_details.equity_amount().value(),
            direction: trade_details.direction(),
            price,
            #[allow(clippy::cast_possible_wrap)]
            block_timestamp: log
                .block_timestamp
                .and_then(|ts| DateTime::from_timestamp(ts as i64, 0)),
            created_at: None,
            gas_used,
            effective_gas_price,
            pyth_price: pyth_pricing.as_ref().map(|p| p.price),
            pyth_confidence: pyth_pricing.as_ref().map(|p| p.confidence),
            pyth_exponent: pyth_pricing.as_ref().map(|p| p.exponent),
            pyth_publish_time: pyth_pricing.as_ref().map(|p| p.publish_time),
        };

        Ok(Some(trade))
    }

    /// Attempts to create an OnchainTrade from a transaction hash by looking up
    /// the transaction receipt and parsing relevant orderbook events.
    pub async fn try_from_tx_hash<P: Provider>(
        tx_hash: B256,
        provider: P,
        cache: &SymbolCache,
        config: &EvmConfig,
        feed_id_cache: &FeedIdCache,
        order_owner: Address,
    ) -> Result<Option<Self>, OnChainError> {
        let receipt = provider
            .get_transaction_receipt(tx_hash)
            .await?
            .ok_or_else(|| {
                OnChainError::Validation(crate::error::TradeValidationError::TransactionNotFound(
                    tx_hash,
                ))
            })?;

        let trades: Vec<_> = receipt
            .inner
            .logs()
            .iter()
            .filter(|log| {
                (log.topic0() == Some(&ClearV3::SIGNATURE_HASH)
                    || log.topic0() == Some(&TakeOrderV3::SIGNATURE_HASH))
                    && log.address() == config.orderbook
            })
            .collect();

        if trades.len() > 1 {
            warn!(
                "Found {} potential trades in the tx with hash {tx_hash}, returning first match",
                trades.len()
            );
        }

        for log in trades {
            if let Some(trade) = try_convert_log_to_onchain_trade(
                log,
                &provider,
                cache,
                config,
                feed_id_cache,
                order_owner,
            )
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

async fn try_convert_log_to_onchain_trade<P: Provider>(
    log: &Log,
    provider: P,
    cache: &SymbolCache,
    config: &EvmConfig,
    feed_id_cache: &FeedIdCache,
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
            config,
            cache,
            &provider,
            clear_event.data().clone(),
            log_with_metadata,
            feed_id_cache,
            order_owner,
        )
        .await;
    }

    if let Ok(take_order_event) = log.log_decode::<TakeOrderV3>() {
        return OnchainTrade::try_from_take_order_if_target_owner(
            cache,
            &provider,
            take_order_event.data().clone(),
            log_with_metadata,
            order_owner,
            feed_id_cache,
        )
        .await;
    }

    Ok(None)
}

/// Converts a Float (bytes32) amount to f64.
///
/// Uses the rain-math-float library's format() method to convert the Float to a string,
/// then parses it to f64. Float.format() handles the proper formatting internally.
fn float_to_f64(float: B256) -> Result<f64, OnChainError> {
    let float = Float::from_raw(float);
    let formatted = float.format()?;
    Ok(formatted.parse::<f64>()?)
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, U256, address, b256, fixed_bytes, uint};
    use alloy::providers::{ProviderBuilder, mock::Asserter};
    use rain_math_float::Float;

    use super::*;
    use crate::bindings::IOrderBookV5;
    use crate::onchain::EvmConfig;
    use crate::symbol::cache::SymbolCache;

    #[test]
    fn test_float_constants_from_v5_interface() {
        // Verify our implementation matches Float constants from LibDecimalFloat.sol

        // FLOAT_ONE = bytes32(uint256(1)) = coefficient=1, exponent=0 → 1.0
        let float_one = B256::from([
            0x00, 0x00, 0x00, 0x00, // exponent = 0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x01, // coefficient = 1
        ]);
        assert!((float_to_f64(float_one).unwrap() - 1.0).abs() < f64::EPSILON);

        // FLOAT_HALF = 0xffffffff...05 = coefficient=5, exponent=-1 → 0.5
        let float_half = B256::from([
            0xff, 0xff, 0xff, 0xff, // exponent = -1
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x05, // coefficient = 5
        ]);
        assert!((float_to_f64(float_half).unwrap() - 0.5).abs() < f64::EPSILON);

        // FLOAT_TWO = bytes32(uint256(2)) = coefficient=2, exponent=0 → 2.0
        let float_two = B256::from([
            0x00, 0x00, 0x00, 0x00, // exponent = 0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x02, // coefficient = 2
        ]);
        assert!((float_to_f64(float_two).unwrap() - 2.0).abs() < f64::EPSILON);
    }

    /// Test with real production event data from tx 0xf05d240c99c7c3f8d4562130f655d2b571b1b82643b621987bed3d8aabab304d
    /// The trade was for 2 shares of tSPLG at ~$80/share.
    ///
    /// TakeOrderV3 event field semantics (counterintuitive naming):
    /// - event.input = amount the order GAVE = 2 tSPLG shares
    /// - event.output = amount the order RECEIVED = ~160 USDC
    #[test]
    fn test_float_to_f64_production_event_data() {
        // event.input Float: 2 shares the order gave
        // Raw bytes: ffffffee00000000000000000000000000000000000000001bc16d674ec80000
        let event_input_float =
            fixed_bytes!("ffffffee00000000000000000000000000000000000000001bc16d674ec80000");
        let shares_amount = float_to_f64(event_input_float).unwrap();
        assert!(
            (shares_amount - 2.0).abs() < f64::EPSILON,
            "Expected 2.0 shares but got {shares_amount}"
        );

        // event.output Float: ~160 USDC the order received
        // Raw bytes: ffffffe500000000000000000000000000000002057d2cd516a29b6174400000
        let event_output_float =
            fixed_bytes!("ffffffe500000000000000000000000000000002057d2cd516a29b6174400000");
        let usdc_amount = float_to_f64(event_output_float).unwrap();
        assert!(
            (usdc_amount - 160.155_077_52).abs() < 0.00001,
            "Expected ~160.15 USDC but got {usdc_amount}"
        );

        // After swapping (as done in take_order.rs):
        // - input_amount (order's input token = USDC) = event.output = 160.15
        // - output_amount (order's output token = tSPLG) = event.input = 2.0
        // Then TradeDetails::try_from_io("USDC", 160.15, "tSPLG", 2.0) correctly extracts:
        // - equity_amount = 2.0 (from output since output is tokenized equity)
        // - usdc_amount = 160.15 (from input since input is USDC)
    }

    #[test]
    fn test_float_to_f64_edge_cases() {
        let float_zero = Float::from_fixed_decimal(uint!(0_U256), 0)
            .unwrap()
            .get_inner();
        assert!((float_to_f64(float_zero).unwrap() - 0.0).abs() < f64::EPSILON);

        let float_one = Float::from_fixed_decimal(uint!(1_U256), 0)
            .unwrap()
            .get_inner();
        assert!((float_to_f64(float_one).unwrap() - 1.0).abs() < f64::EPSILON);

        let float_nine = Float::from_fixed_decimal(uint!(9_U256), 0)
            .unwrap()
            .get_inner();
        let result = float_to_f64(float_nine).unwrap();
        assert!((result - 9.0).abs() < f64::EPSILON);

        let float_hundred = Float::from_fixed_decimal(uint!(100_U256), 0)
            .unwrap()
            .get_inner();
        let result = float_to_f64(float_hundred).unwrap();
        assert!((result - 100.0).abs() < f64::EPSILON);

        let float_half = Float::from_fixed_decimal(uint!(5_U256), 1)
            .unwrap()
            .get_inner();
        let result = float_to_f64(float_half).unwrap();
        assert!((result - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_float_to_f64_precision_loss() {
        // Test with very large coefficient
        let large_coeff = 1_000_000_000_000_000_i128;
        let float_large = Float::from_fixed_decimal_lossy(U256::from(large_coeff), 0)
            .unwrap()
            .get_inner();
        let result = float_to_f64(float_large).unwrap();
        assert!(result.is_finite());
        assert!((result - 1_000_000_000_000_000.0).abs() < 1.0);

        // Test with very small value (high negative exponent)
        let float_small = Float::from_fixed_decimal_lossy(uint!(1_U256), 50)
            .unwrap()
            .get_inner();
        let result = float_to_f64(float_small).unwrap();
        assert!(result.is_finite());
        // 1 × 10^-50 is extremely small
        assert!(result > 0.0 && result < 1e-40);
    }

    #[test]
    fn test_float_to_f64_formatting_edge_cases() {
        let float_amount = Float::from_fixed_decimal(uint!(123_456_U256), 6)
            .unwrap()
            .get_inner();
        let result = float_to_f64(float_amount).unwrap();
        assert!((result - 0.123_456).abs() < f64::EPSILON);

        let float_amount = Float::from_fixed_decimal(uint!(5_U256), 10)
            .unwrap()
            .get_inner();
        let result = float_to_f64(float_amount).unwrap();
        assert!((result - 5e-10).abs() < 1e-15);

        let float_amount = Float::from_fixed_decimal(uint!(12_345_U256), 0)
            .unwrap()
            .get_inner();
        let result = float_to_f64(float_amount).unwrap();
        assert!((result - 12_345.0).abs() < f64::EPSILON);

        let float_amount = Float::from_fixed_decimal(uint!(5000_U256), 0)
            .unwrap()
            .get_inner();
        let result = float_to_f64(float_amount).unwrap();
        assert!((result - 5000.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_try_from_tx_hash_transaction_not_found() {
        let asserter = Asserter::new();
        // Mock the eth_getTransactionReceipt call to return null (transaction not found)
        asserter.push_success(&serde_json::Value::Null);
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let cache = SymbolCache::default();
        let feed_id_cache = FeedIdCache::default();
        let config = EvmConfig {
            ws_rpc_url: "ws://localhost:8545".parse().unwrap(),
            orderbook: Address::ZERO,
            order_owner: Some(Address::ZERO),
            deployment_block: 0,
        };

        let tx_hash =
            fixed_bytes!("0x4444444444444444444444444444444444444444444444444444444444444444");

        // Mock returns empty response by default, simulating transaction not found
        let result = OnchainTrade::try_from_tx_hash(
            tx_hash,
            provider,
            &cache,
            &config,
            &feed_id_cache,
            Address::ZERO,
        )
        .await;

        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(TradeValidationError::TransactionNotFound(_))
        ));
    }

    fn make_io(token: Address, vault_id: B256) -> IOrderBookV5::IOV2 {
        IOrderBookV5::IOV2 {
            token,
            vaultId: vault_id,
        }
    }

    fn make_order(
        owner: Address,
        inputs: Vec<IOrderBookV5::IOV2>,
        outputs: Vec<IOrderBookV5::IOV2>,
    ) -> IOrderBookV5::OrderV4 {
        IOrderBookV5::OrderV4 {
            owner,
            evaluable: IOrderBookV5::EvaluableV4::default(),
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

        let event = IOrderBookV5::ClearV3 {
            sender: address!("0x0000000000000000000000000000000000000000"),
            alice,
            bob,
            clearConfig: IOrderBookV5::ClearConfigV2 {
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

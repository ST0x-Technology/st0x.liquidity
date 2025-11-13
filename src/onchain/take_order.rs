use alloy::providers::Provider;
use alloy::rpc::types::Log;

use crate::bindings::IOrderBookV5::{TakeOrderConfigV4, TakeOrderV3};
use crate::error::OnChainError;
use crate::onchain::pyth::FeedIdCache;
use crate::onchain::trade::{OnchainTrade, OrderFill};
use crate::symbol::cache::SymbolCache;

impl OnchainTrade {
    /// Creates OnchainTrade directly from TakeOrderV3 blockchain events
    #[tracing::instrument(skip_all, fields(tx_hash = ?log.transaction_hash, log_index = ?log.log_index), level = tracing::Level::DEBUG)]
    pub async fn try_from_take_order_if_target_owner<P: Provider>(
        cache: &SymbolCache,
        provider: P,
        event: TakeOrderV3,
        log: Log,
        target_order_owner: alloy::primitives::Address,
        feed_id_cache: &FeedIdCache,
    ) -> Result<Option<Self>, OnChainError> {
        if event.config.order.owner != target_order_owner {
            return Ok(None);
        }

        let TakeOrderConfigV4 {
            order,
            inputIOIndex,
            outputIOIndex,
            signedContext: _,
        } = event.config;

        let fill = OrderFill {
            input_index: usize::try_from(inputIOIndex)?,
            input_amount: event.input,
            output_index: usize::try_from(outputIOIndex)?,
            output_amount: event.output,
        };

        Self::try_from_order_and_fill_details(cache, &provider, order, fill, log, feed_id_cache)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bindings::IERC20::{decimalsCall, symbolCall};
    use crate::bindings::IOrderBookV5::{SignedContextV1, TakeOrderConfigV4, TakeOrderV3};
    use crate::onchain::pyth::FeedIdCache;
    use crate::symbol::cache::SymbolCache;
    use crate::test_utils::{get_test_log, get_test_order};
    use crate::tokenized_symbol;
    use alloy::hex;
    use alloy::primitives::{I256, U256, address, fixed_bytes};
    use alloy::providers::{ProviderBuilder, mock::Asserter};
    use alloy::sol_types::SolCall;
    use rain_math_float::Float;
    use std::str::FromStr;

    fn create_take_order_event_with_order(
        order: crate::bindings::IOrderBookV5::OrderV4,
    ) -> TakeOrderV3 {
        // Helper to create Float from coefficient and exponent
        fn create_float(coefficient: i128, exponent: i32) -> alloy::primitives::B256 {
            let mut bytes = [0u8; 32];
            bytes[0..4].copy_from_slice(&exponent.to_be_bytes());

            let coeff_i256 = I256::try_from(coefficient).expect("coefficient fits");
            let coeff_bytes = coeff_i256.to_be_bytes::<32>();
            bytes[4..32].copy_from_slice(&coeff_bytes[4..32]);

            alloy::primitives::B256::from(bytes)
        }

        TakeOrderV3 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            config: TakeOrderConfigV4 {
                order,
                inputIOIndex: U256::from(0),
                outputIOIndex: U256::from(1),
                signedContext: vec![SignedContextV1 {
                    signer: address!("0x0000000000000000000000000000000000000000"),
                    signature: vec![].into(),
                    context: vec![],
                }],
            },
            // 100 USDC: coefficient=100, exponent=0
            input: create_float(100, 0),
            // 9 shares: coefficient=9, exponent=0
            output: create_float(9, 0),
        }
    }

    fn mocked_receipt_hex(tx_hash: alloy::primitives::FixedBytes<32>) -> serde_json::Value {
        serde_json::json!({
            "transactionHash": hex::encode_prefixed(tx_hash),
            "transactionIndex": "0x1",
            "blockHash": "0x1234567890123456789012345678901234567890123456789012345678901234",
            "blockNumber": "0x1",
            "from": "0x1234567890123456789012345678901234567890",
            "to": "0x5678901234567890123456789012345678901234",
            "gasUsed": "0x5208",
            "effectiveGasPrice": "0x77359400",
            "cumulativeGasUsed": "0x5208",
            "status": "0x1",
            "type": "0x2",
            "logsBloom": "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
            "logs": []
        })
    }

    #[tokio::test]
    async fn test_try_from_take_order_if_target_owner_match() {
        let cache = SymbolCache::default();
        let order = get_test_order();
        let target_order_owner = order.owner;

        let take_event = create_take_order_event_with_order(order);
        let log = get_test_log();

        let asserter = Asserter::new();

        let tx_hash =
            fixed_bytes!("0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
        asserter.push_success(&mocked_receipt_hex(tx_hash));
        // Mock decimals() then symbol() calls in the order they're called for input token (USDC)
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&6u8)); // USDC decimals
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"USDC".to_string(),
        ));
        // Mock decimals() then symbol() calls for output token (AAPL0x)
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&18u8)); // AAPL0x decimals
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"AAPL0x".to_string(),
        ));

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let feed_id_cache = FeedIdCache::default();

        let result = OnchainTrade::try_from_take_order_if_target_owner(
            &cache,
            provider,
            take_event,
            log,
            target_order_owner,
            &feed_id_cache,
        )
        .await
        .unwrap();

        let trade = result.unwrap();
        assert_eq!(trade.symbol, tokenized_symbol!("AAPL0x"));
        assert!((trade.amount - 9.0).abs() < f64::EPSILON);
        assert_eq!(
            trade.tx_hash,
            fixed_bytes!("0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
        );
        assert_eq!(trade.log_index, 293);
    }

    #[tokio::test]
    async fn test_try_from_take_order_if_target_owner_no_match() {
        let cache = SymbolCache::default();
        let order = get_test_order();

        // Create a different target owner that won't match
        let different_target_owner = address!("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");

        let take_event = create_take_order_event_with_order(order);
        let log = get_test_log();

        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let feed_id_cache = FeedIdCache::default();

        let result = OnchainTrade::try_from_take_order_if_target_owner(
            &cache,
            &provider,
            take_event,
            log,
            different_target_owner,
            &feed_id_cache,
        )
        .await
        .unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_try_from_take_order_if_target_owner_different_input_output_indices() {
        let cache = SymbolCache::default();
        let order = get_test_order();
        let target_order_owner = order.owner;

        // Helper to create Float for testing
        fn create_float(value: i128, decimals: u8) -> alloy::primitives::B256 {
            let u256_value = U256::from(value.unsigned_abs());
            let float = Float::from_fixed_decimal_lossy(u256_value, decimals).expect("valid Float");
            float.get_inner()
        }

        let take_event = TakeOrderV3 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            config: TakeOrderConfigV4 {
                order,
                inputIOIndex: U256::from(1), // Different indices
                outputIOIndex: U256::from(0),
                signedContext: vec![SignedContextV1 {
                    signer: address!("0x0000000000000000000000000000000000000000"),
                    signature: vec![].into(),
                    context: vec![],
                }],
            },
            // 5 shares: coefficient=5, exponent=0
            input: create_float(5, 0),
            // 50 USDC: coefficient=50, exponent=0
            output: create_float(50, 0),
        };

        let log = get_test_log();

        let asserter = Asserter::new();

        let tx_hash =
            fixed_bytes!("0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
        asserter.push_success(&mocked_receipt_hex(tx_hash));
        // Mock decimals() then symbol() calls in the order they're called for input token (AAPL0x)
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&18u8)); // AAPL0x decimals
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"AAPL0x".to_string(),
        ));
        // Mock decimals() then symbol() calls for output token (USDC)
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&6u8)); // USDC decimals
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"USDC".to_string(),
        ));

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let feed_id_cache = FeedIdCache::default();

        let result = OnchainTrade::try_from_take_order_if_target_owner(
            &cache,
            provider,
            take_event,
            log,
            target_order_owner,
            &feed_id_cache,
        )
        .await
        .unwrap();

        let trade = result.unwrap();
        assert_eq!(trade.symbol, tokenized_symbol!("AAPL0x"));
        assert!((trade.amount - 5.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_try_from_take_order_if_target_owner_with_different_amounts() {
        let cache = SymbolCache::default();
        let order = get_test_order();
        let target_order_owner = order.owner;

        // Helper to create Float for testing
        fn create_float(value: i128, decimals: u8) -> alloy::primitives::B256 {
            let u256_value = U256::from(value.unsigned_abs());
            let float = Float::from_fixed_decimal_lossy(u256_value, decimals).expect("valid Float");
            float.get_inner()
        }

        let take_event = TakeOrderV3 {
            sender: address!("0x2222222222222222222222222222222222222222"),
            config: TakeOrderConfigV4 {
                order,
                inputIOIndex: U256::from(0),
                outputIOIndex: U256::from(1),
                signedContext: vec![SignedContextV1 {
                    signer: address!("0x0000000000000000000000000000000000000000"),
                    signature: vec![].into(),
                    context: vec![],
                }],
            },
            // 200 USDC: coefficient=200, exponent=0
            input: create_float(200, 0),
            // 15 shares: coefficient=15, exponent=0
            output: create_float(15, 0),
        };

        let log = get_test_log();

        let asserter = Asserter::new();

        let tx_hash =
            fixed_bytes!("0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
        asserter.push_success(&mocked_receipt_hex(tx_hash));
        // Mock decimals() then symbol() calls in the order they're called for input token (USDC)
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&6u8)); // USDC decimals
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"USDC".to_string(),
        ));
        // Mock decimals() then symbol() calls for output token (AAPL0x)
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&18u8)); // AAPL0x decimals
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"AAPL0x".to_string(),
        ));

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let feed_id_cache = FeedIdCache::default();

        let result = OnchainTrade::try_from_take_order_if_target_owner(
            &cache,
            provider,
            take_event,
            log,
            target_order_owner,
            &feed_id_cache,
        )
        .await
        .unwrap();

        let trade = result.unwrap();
        assert_eq!(trade.symbol, tokenized_symbol!("AAPL0x"));
        assert!((trade.amount - 15.0).abs() < f64::EPSILON);
        // Price should be 200 USDC / 15 shares = 13.333... USDC per share
        assert!((trade.price_usdc - 13.333_333_333_333_334).abs() < 0.001);
    }

    #[tokio::test]
    async fn test_try_from_take_order_if_target_owner_zero_amounts() {
        let cache = SymbolCache::default();
        let order = get_test_order();
        let target_order_owner = order.owner;

        let take_event = TakeOrderV3 {
            sender: address!("0x3333333333333333333333333333333333333333"),
            config: TakeOrderConfigV4 {
                order,
                inputIOIndex: U256::from(0),
                outputIOIndex: U256::from(1),
                signedContext: vec![SignedContextV1 {
                    signer: address!("0x0000000000000000000000000000000000000000"),
                    signature: vec![].into(),
                    context: vec![],
                }],
            },
            input: alloy::primitives::B256::ZERO,  // Zero input
            output: alloy::primitives::B256::ZERO, // Zero output
        };

        let log = get_test_log();

        let asserter = Asserter::new();

        let tx_hash =
            fixed_bytes!("0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
        asserter.push_success(&mocked_receipt_hex(tx_hash));
        // Mock decimals() then symbol() calls in the order they're called for input token (USDC)
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&6u8)); // USDC decimals
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"USDC".to_string(),
        ));
        // Mock decimals() then symbol() calls for output token (AAPL0x)
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&18u8)); // AAPL0x decimals
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"AAPL0x".to_string(),
        ));

        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let feed_id_cache = FeedIdCache::default();

        let result = OnchainTrade::try_from_take_order_if_target_owner(
            &cache,
            provider,
            take_event,
            log,
            target_order_owner,
            &feed_id_cache,
        )
        .await;

        // Zero amounts should deterministically return Ok(None)
        assert_eq!(result.unwrap(), None);
    }

    #[tokio::test]
    async fn test_try_from_take_order_if_target_owner_invalid_io_index() {
        let cache = SymbolCache::default();
        let order = get_test_order();
        let target_order_owner = order.owner;

        let take_event = TakeOrderV3 {
            sender: address!("0x4444444444444444444444444444444444444444"),
            config: TakeOrderConfigV4 {
                order,
                inputIOIndex: U256::from(99), // Invalid index (order only has 2 IOs)
                outputIOIndex: U256::from(1),
                signedContext: vec![SignedContextV1 {
                    signer: address!("0x0000000000000000000000000000000000000000"),
                    signature: vec![].into(),
                    context: vec![],
                }],
            },
            input: alloy::primitives::B256::new(U256::from(100_000_000u64).to_le_bytes::<32>()),
            output: alloy::primitives::B256::new(
                U256::from_str("9000000000000000000").unwrap().to_le_bytes::<32>(),
            ),
        };

        let log = get_test_log();

        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let feed_id_cache = FeedIdCache::default();

        let result = OnchainTrade::try_from_take_order_if_target_owner(
            &cache,
            provider,
            take_event,
            log,
            target_order_owner,
            &feed_id_cache,
        )
        .await;

        // Should return an error due to invalid IO index
        assert!(result.is_err());
    }
}

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
    pub async fn try_from_take_order_if_target_owner<P: Provider + Clone>(
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

        let input_index = usize::try_from(inputIOIndex)?;
        let output_index = usize::try_from(outputIOIndex)?;

        let fill = OrderFill {
            input_index,
            input_amount: event.input.into(),
            output_index,
            output_amount: event.output.into(),
        };

        Self::try_from_order_v4_and_fill_details(cache, provider, order, fill, log, feed_id_cache)
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
    use crate::test_utils::{get_test_log, get_test_order_v4};
    use crate::tokenized_symbol;
    use alloy::hex;
    use alloy::primitives::{U256, address, fixed_bytes};
    use alloy::providers::{ProviderBuilder, mock::Asserter};
    use alloy::sol_types::SolCall;
    use std::str::FromStr;

    fn create_take_order_event_with_order(
        order: crate::bindings::IOrderBookV5::OrderV4,
    ) -> TakeOrderV3 {
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
            input: U256::from(100_000_000u64).into(), // 100 USDC (6 decimals)
            output: U256::from_str("9000000000000000000").unwrap().into(), // 9 shares (18 decimals)
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
        let order = get_test_order_v4();
        let target_order_owner = order.owner;

        let take_event = create_take_order_event_with_order(order);
        let log = get_test_log();

        let asserter = Asserter::new();

        let tx_hash =
            fixed_bytes!("0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
        asserter.push_success(&mocked_receipt_hex(tx_hash));
        // decimals() call for input token (USDC)
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&6u8));
        // decimals() call for output token (AAPL0x)
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&18u8));
        // symbol() call for input token (USDC)
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"USDC".to_string(),
        ));
        // symbol() call for output token (AAPL0x)
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
        let order = get_test_order_v4();

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
        let order = get_test_order_v4();
        let target_order_owner = order.owner;

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
            input: U256::from_str("5000000000000000000").unwrap().into(), // 5 shares (18 decimals)
            output: U256::from(50_000_000u64).into(),                     // 50 USDC (6 decimals)
        };

        let log = get_test_log();

        let asserter = Asserter::new();

        let tx_hash =
            fixed_bytes!("0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
        asserter.push_success(&mocked_receipt_hex(tx_hash));
        // decimals() call for input token (at index 1, AAPL0x with 18 decimals)
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&18u8));
        // decimals() call for output token (at index 0, USDC with 6 decimals)
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&6u8));
        // symbol() call for input token (AAPL0x)
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"AAPL0x".to_string(),
        ));
        // symbol() call for output token (USDC)
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
        let order = get_test_order_v4();
        let target_order_owner = order.owner;

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
            input: U256::from(200_000_000u64).into(), // 200 USDC
            output: U256::from_str("15000000000000000000").unwrap().into(), // 15 shares
        };

        let log = get_test_log();

        let asserter = Asserter::new();

        let tx_hash =
            fixed_bytes!("0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
        asserter.push_success(&mocked_receipt_hex(tx_hash));
        // decimals() call for input token (USDC with 6 decimals)
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&6u8));
        // decimals() call for output token (AAPL0x with 18 decimals)
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&18u8));
        // symbol() call for input token (USDC)
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"USDC".to_string(),
        ));
        // symbol() call for output token (AAPL0x)
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
        let order = get_test_order_v4();
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
            input: U256::ZERO.into(),  // Zero input
            output: U256::ZERO.into(), // Zero output
        };

        let log = get_test_log();

        let asserter = Asserter::new();

        let tx_hash =
            fixed_bytes!("0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
        asserter.push_success(&mocked_receipt_hex(tx_hash));
        // decimals() call for input token (USDC with 6 decimals)
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&6u8));
        // decimals() call for output token (AAPL0x with 18 decimals)
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&18u8));
        // symbol() call for input token (USDC)
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"USDC".to_string(),
        ));
        // symbol() call for output token (AAPL0x)
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
        let order = get_test_order_v4();
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
            input: U256::from(100_000_000u64).into(),
            output: U256::from_str("9000000000000000000").unwrap().into(),
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

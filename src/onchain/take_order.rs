//! Processes TakeOrderV3 events from the Raindex orderbook.

use alloy::primitives::Address;
use alloy::rpc::types::Log;

use st0x_evm::Evm;

use super::OnChainError;
use crate::bindings::IOrderBookV6::{TakeOrderConfigV4, TakeOrderV3};
use crate::onchain::pyth::FeedIdCache;
use crate::onchain::trade::{OnchainTrade, OrderFill};
use crate::symbol::cache::SymbolCache;

impl OnchainTrade {
    /// Creates OnchainTrade directly from TakeOrderV3 blockchain events
    #[tracing::instrument(skip_all, fields(tx_hash = ?log.transaction_hash, log_index = ?log.log_index), level = tracing::Level::DEBUG)]
    pub async fn try_from_take_order_if_target_owner<E: Evm>(
        cache: &SymbolCache,
        evm: &E,
        event: TakeOrderV3,
        log: Log,
        target_order_owner: Address,
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

        // Per IOrderBookV6.sol lines 385-386, TakeOrderV3's `input`/`output` are
        // "from the perspective of sender" (the taker), NOT the order:
        // - event.input = what taker received = what order GAVE (order's output)
        // - event.output = what taker gave = what order RECEIVED (order's input)
        // We swap them to match the order's validInputs/validOutputs perspective.
        let fill = OrderFill {
            input_index: usize::try_from(inputIOIndex)?,
            input_amount: event.output,
            output_index: usize::try_from(outputIOIndex)?,
            output_amount: event.input,
        };

        Self::try_from_order_and_fill_details(cache, evm, order, fill, log, feed_id_cache).await
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{B256, TxHash, U256, address, fixed_bytes, uint};
    use alloy::providers::{ProviderBuilder, mock::Asserter};
    use alloy::sol_types::SolCall;
    use rain_math_float::Float;
    use rust_decimal_macros::dec;

    use st0x_evm::ReadOnlyEvm;
    use st0x_execution::FractionalShares;

    use super::*;
    use crate::bindings::IERC20::{decimalsCall, symbolCall};
    use crate::bindings::IOrderBookV6::{SignedContextV1, TakeOrderConfigV4, TakeOrderV3};
    use crate::onchain::pyth::FeedIdCache;
    use crate::symbol::cache::SymbolCache;
    use crate::test_utils::{get_test_log, get_test_order};
    use crate::tokenized_symbol;

    fn create_take_order_event_with_order(
        order: crate::bindings::IOrderBookV6::OrderV4,
    ) -> TakeOrderV3 {
        // Per IOrderBookV6.sol lines 385-386, input/output are from taker's perspective.
        // For a trade where order receives 100 USDC and gives 9 shares:
        // - input = 9 (taker received 9 shares = order gave 9 shares)
        // - output = 100 (taker gave 100 USDC = order received 100 USDC)
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
            input: Float::from_fixed_decimal_lossy(uint!(9_U256), 0)
                .unwrap()
                .0
                .get_inner(),
            output: Float::from_fixed_decimal_lossy(uint!(100_U256), 0)
                .unwrap()
                .0
                .get_inner(),
        }
    }

    fn mocked_receipt_hex(tx_hash: TxHash) -> serde_json::Value {
        serde_json::json!({
            "transactionHash": tx_hash,
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
        // Mock decimals() then symbol() calls for output token (tAAPL)
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&18u8)); // tAAPL decimals
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"tAAPL".to_string(),
        ));

        let evm = ReadOnlyEvm::new(ProviderBuilder::new().connect_mocked_client(asserter));
        let feed_id_cache = FeedIdCache::default();

        let result = OnchainTrade::try_from_take_order_if_target_owner(
            &cache,
            &evm,
            take_event,
            log,
            target_order_owner,
            &feed_id_cache,
        )
        .await
        .unwrap();

        let trade = result.unwrap();
        assert_eq!(trade.symbol, tokenized_symbol!("tAAPL"));
        assert_eq!(trade.amount, FractionalShares::new(dec!(9)));
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
        let evm = ReadOnlyEvm::new(ProviderBuilder::new().connect_mocked_client(asserter));
        let feed_id_cache = FeedIdCache::default();

        let result = OnchainTrade::try_from_take_order_if_target_owner(
            &cache,
            &evm,
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

        // Swapped indices: input from validInputs[1]=tAAPL, output from validOutputs[0]=USDC
        // Order receives 5 tAAPL and gives 50 USDC
        // event.input = what order gave = 50 USDC
        // event.output = what order received = 5 tAAPL
        let take_event = TakeOrderV3 {
            sender: address!("0x1111111111111111111111111111111111111111"),
            config: TakeOrderConfigV4 {
                order,
                inputIOIndex: U256::from(1),
                outputIOIndex: U256::from(0),
                signedContext: vec![SignedContextV1 {
                    signer: address!("0x0000000000000000000000000000000000000000"),
                    signature: vec![].into(),
                    context: vec![],
                }],
            },
            input: Float::from_fixed_decimal_lossy(uint!(50_U256), 0)
                .unwrap()
                .0
                .get_inner(),
            output: Float::from_fixed_decimal_lossy(uint!(5_U256), 0)
                .unwrap()
                .0
                .get_inner(),
        };

        let log = get_test_log();

        let asserter = Asserter::new();

        let tx_hash =
            fixed_bytes!("0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
        asserter.push_success(&mocked_receipt_hex(tx_hash));
        // Mock decimals() then symbol() calls in the order they're called for input token (tAAPL)
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&18u8)); // tAAPL decimals
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"tAAPL".to_string(),
        ));
        // Mock decimals() then symbol() calls for output token (USDC)
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&6u8)); // USDC decimals
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"USDC".to_string(),
        ));

        let evm = ReadOnlyEvm::new(ProviderBuilder::new().connect_mocked_client(asserter));
        let feed_id_cache = FeedIdCache::default();

        let result = OnchainTrade::try_from_take_order_if_target_owner(
            &cache,
            &evm,
            take_event,
            log,
            target_order_owner,
            &feed_id_cache,
        )
        .await
        .unwrap();

        let trade = result.unwrap();
        assert_eq!(trade.symbol, tokenized_symbol!("tAAPL"));
        assert_eq!(trade.amount, FractionalShares::new(dec!(5)));
    }

    #[tokio::test]
    async fn test_try_from_take_order_if_target_owner_with_different_amounts() {
        let cache = SymbolCache::default();
        let order = get_test_order();
        let target_order_owner = order.owner;

        // Order receives 200 USDC (validInputs[0]) and gives 15 shares (validOutputs[1])
        // event.input = what order gave = 15 shares
        // event.output = what order received = 200 USDC
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
            input: Float::from_fixed_decimal_lossy(uint!(15_U256), 0)
                .unwrap()
                .0
                .get_inner(),
            output: Float::from_fixed_decimal_lossy(uint!(200_U256), 0)
                .unwrap()
                .0
                .get_inner(),
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
        // Mock decimals() then symbol() calls for output token (wtAAPL)
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&18u8)); // wtAAPL decimals
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"wtAAPL".to_string(),
        ));

        let evm = ReadOnlyEvm::new(ProviderBuilder::new().connect_mocked_client(asserter));
        let feed_id_cache = FeedIdCache::default();

        let result = OnchainTrade::try_from_take_order_if_target_owner(
            &cache,
            &evm,
            take_event,
            log,
            target_order_owner,
            &feed_id_cache,
        )
        .await
        .unwrap();

        let trade = result.unwrap();
        assert_eq!(
            trade.symbol,
            tokenized_symbol!(WrappedTokenizedShares, "wtAAPL")
        );
        assert_eq!(trade.amount, FractionalShares::new(dec!(15)));
        // Price should be 200 USDC / 15 shares = 13.333... USDC per share
        assert_eq!(trade.price.value(), dec!(200) / dec!(15));
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
            input: B256::ZERO,  // Zero input
            output: B256::ZERO, // Zero output
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
        // Mock decimals() then symbol() calls for output token (tAAPL)
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&18u8)); // tAAPL decimals
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"tAAPL".to_string(),
        ));

        let evm = ReadOnlyEvm::new(ProviderBuilder::new().connect_mocked_client(asserter));
        let feed_id_cache = FeedIdCache::default();

        let result = OnchainTrade::try_from_take_order_if_target_owner(
            &cache,
            &evm,
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

        // Helper to create Float for testing using from_fixed_decimal_lossy
        fn create_float(value: u64, decimals: u8) -> B256 {
            let u256_value = U256::from(value);
            let (float, _lossy) =
                Float::from_fixed_decimal_lossy(u256_value, decimals).expect("valid Float");
            float.get_inner()
        }

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
            input: create_float(100_000_000, 0),
            output: create_float(9_000_000_000_000_000_000, 18),
        };

        let log = get_test_log();

        let asserter = Asserter::new();
        let evm = ReadOnlyEvm::new(ProviderBuilder::new().connect_mocked_client(asserter));
        let feed_id_cache = FeedIdCache::default();

        let result = OnchainTrade::try_from_take_order_if_target_owner(
            &cache,
            &evm,
            take_event,
            log,
            target_order_owner,
            &feed_id_cache,
        )
        .await;

        // Should return an error due to invalid IO index
        result.unwrap_err();
    }
}

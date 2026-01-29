use alloy::primitives::Address;
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, Log};
use alloy::sol_types::SolEvent;
use tracing::{debug, info};

use crate::bindings::IOrderBookV5::{AfterClearV2, ClearConfigV2, ClearStateChangeV2, ClearV3};
use crate::error::{OnChainError, TradeValidationError};
use crate::onchain::{
    EvmConfig,
    pyth::FeedIdCache,
    trade::{OnchainTrade, OrderFill},
};
use crate::symbol::cache::SymbolCache;

impl OnchainTrade {
    /// Creates OnchainTrade directly from ClearV3 blockchain events
    #[tracing::instrument(skip_all, fields(tx_hash = ?log.transaction_hash, log_index = ?log.log_index), level = tracing::Level::DEBUG)]
    pub async fn try_from_clear_v3<P: Provider>(
        config: &EvmConfig,
        cache: &SymbolCache,
        provider: P,
        event: ClearV3,
        log: Log,
        feed_id_cache: &FeedIdCache,
        order_owner: Address,
    ) -> Result<Option<Self>, OnChainError> {
        let ClearV3 {
            sender: _,
            alice: alice_order,
            bob: bob_order,
            clearConfig: clear_config,
        } = event;

        let ClearConfigV2 {
            aliceInputIOIndex,
            aliceOutputIOIndex,
            bobInputIOIndex,
            bobOutputIOIndex,
            ..
        } = clear_config;

        let alice_owner_matches = alice_order.owner == order_owner;
        let bob_owner_matches = bob_order.owner == order_owner;

        debug!(
            "ClearV3 owner comparison: alice.owner={:?}, bob.owner={:?}, order_owner={:?}, alice_matches={}, bob_matches={}",
            alice_order.owner, bob_order.owner, order_owner, alice_owner_matches, bob_owner_matches
        );

        if !(alice_owner_matches || bob_owner_matches) {
            info!(
                "ClearV3 event filtered (no owner match): tx_hash={:?}, log_index={}, alice.owner={:?}, bob.owner={:?}, target={:?}",
                log.transaction_hash,
                log.log_index.unwrap_or(0),
                alice_order.owner,
                bob_order.owner,
                order_owner
            );
            return Ok(None);
        }

        let after_clear = fetch_after_clear_event(&provider, config, &log).await?;

        let ClearStateChangeV2 {
            aliceOutput,
            bobOutput,
            aliceInput,
            bobInput,
        } = after_clear.clearStateChange;

        let (order, fill) = if alice_owner_matches {
            let fill = OrderFill {
                input_index: usize::try_from(aliceInputIOIndex)?,
                input_amount: aliceInput,
                output_index: usize::try_from(aliceOutputIOIndex)?,
                output_amount: aliceOutput,
            };
            (alice_order, fill)
        } else {
            let fill = OrderFill {
                input_index: usize::try_from(bobInputIOIndex)?,
                input_amount: bobInput,
                output_index: usize::try_from(bobOutputIOIndex)?,
                output_amount: bobOutput,
            };
            (bob_order, fill)
        };

        let result = Self::try_from_order_and_fill_details(
            cache,
            &provider,
            order,
            fill,
            log,
            feed_id_cache,
        )
        .await;

        if let Ok(Some(ref trade)) = result {
            info!(
                "ClearV3 trade created successfully: tx_hash={tx_hash:?}, log_index={log_index}, symbol={symbol}, amount={amount}, direction={direction:?}",
                tx_hash = trade.tx_hash,
                log_index = trade.log_index,
                symbol = trade.symbol,
                amount = trade.amount,
                direction = trade.direction
            );
        }

        result
    }
}

/// Fetches the AfterClearV2 event that corresponds to a ClearV3 event.
///
/// The smart contract always emits ClearV3 followed immediately by AfterClearV2 in the same
/// transaction, so AfterClearV2 should always exist with a higher log index.
///
/// # Node Provider Workaround
///
/// We've observed that some RPC nodes return incomplete results from `eth_getLogs` for
/// historical blocks, even when the logs exist in the transaction receipt. When `get_logs`
/// fails to find the AfterClearV2, we fall back to extracting it directly from the
/// transaction receipt. This was discovered during backfill operations where `get_logs`
/// returned 0 AfterClearV2 logs, but `get_transaction_receipt` showed both logs present.
async fn fetch_after_clear_event<P: Provider>(
    provider: &P,
    config: &EvmConfig,
    log: &Log,
) -> Result<AfterClearV2, OnChainError> {
    let block_number = log
        .block_number
        .ok_or(TradeValidationError::NoBlockNumber)?;
    let tx_hash = log.transaction_hash.ok_or(TradeValidationError::NoTxHash)?;
    let clear_log_index = log.log_index.ok_or(TradeValidationError::NoLogIndex)?;

    let filter = Filter::new()
        .select(block_number)
        .address(config.orderbook)
        .event_signature(AfterClearV2::SIGNATURE_HASH);

    let after_clear_logs = provider.get_logs(&filter).await?;

    if let Some(after_clear_log) = after_clear_logs.iter().find(|after_clear_log| {
        after_clear_log.transaction_hash == log.transaction_hash
            && after_clear_log.log_index > log.log_index
    }) {
        return Ok(after_clear_log.log_decode::<AfterClearV2>()?.data().clone());
    }

    // Fallback: extract from tx receipt when get_logs returns incomplete data.
    // Some RPC nodes fail to return logs via eth_getLogs for historical blocks,
    // but the logs are present in the transaction receipt.
    let receipt = provider.get_transaction_receipt(tx_hash).await?;
    let Some(r) = receipt else {
        return Err(TradeValidationError::NodeReceiptMissing {
            block_number,
            tx_hash,
            clear_log_index,
        }
        .into());
    };

    // Find the AfterClearV2 log in the receipt with log_index > clear_log_index
    for receipt_log in r.inner.logs() {
        if receipt_log.address() != config.orderbook {
            continue;
        }

        if receipt_log.topics().first() != Some(&AfterClearV2::SIGNATURE_HASH) {
            continue;
        }

        let Some(receipt_log_index) = receipt_log.log_index else {
            continue;
        };

        if receipt_log_index <= clear_log_index {
            continue;
        }

        // Found it - decode and return
        let decoded = receipt_log.log_decode::<AfterClearV2>()?;
        debug!(
            block_number,
            %tx_hash,
            clear_log_index,
            receipt_log_index,
            "Extracted AfterClearV2 from tx receipt (get_logs returned incomplete data)"
        );
        return Ok(decoded.data().clone());
    }

    // Check what's actually in the receipt for error reporting
    let clear_in_receipt = r.inner.logs().iter().any(|l| {
        l.address() == config.orderbook && l.topics().first() == Some(&ClearV3::SIGNATURE_HASH)
    });
    let after_clear_in_receipt = r.inner.logs().iter().any(|l| {
        l.address() == config.orderbook && l.topics().first() == Some(&AfterClearV2::SIGNATURE_HASH)
    });

    if clear_in_receipt && !after_clear_in_receipt {
        return Err(TradeValidationError::AfterClearMissingFromReceipt {
            block_number,
            tx_hash,
            clear_log_index,
        }
        .into());
    }

    // Neither log in receipt or AfterClearV2 has wrong log index - node issue
    Err(TradeValidationError::NodeReceiptMissing {
        block_number,
        tx_hash,
        clear_log_index,
    }
    .into())
}

#[cfg(test)]
mod tests {
    use alloy::hex;
    use alloy::primitives::{
        Address, B256, Bytes, IntoLogData, LogData, U256, address, fixed_bytes, uint,
    };
    use alloy::providers::{ProviderBuilder, mock::Asserter};
    use alloy::rpc::types::Log;
    use alloy::sol_types::SolCall;
    use rain_math_float::Float;
    use serde_json::json;

    use super::*;
    use crate::bindings::IERC20::{decimalsCall, symbolCall};
    use crate::bindings::IOrderBookV5;
    use crate::bindings::IOrderBookV5::{AfterClearV2, ClearConfigV2, ClearStateChangeV2};
    use crate::onchain::pyth::FeedIdCache;
    use crate::symbol::cache::SymbolCache;
    use crate::test_utils::{get_test_log, get_test_order};
    use crate::tokenized_symbol;

    fn create_test_config() -> EvmConfig {
        EvmConfig {
            ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            order_owner: Some(get_test_order().owner),
            deployment_block: 1,
        }
    }

    fn create_clear_event(
        alice_order: IOrderBookV5::OrderV4,
        bob_order: IOrderBookV5::OrderV4,
    ) -> ClearV3 {
        ClearV3 {
            sender: address!("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"),
            alice: alice_order,
            bob: bob_order,
            clearConfig: ClearConfigV2 {
                aliceInputIOIndex: U256::from(0),
                aliceOutputIOIndex: U256::from(1),
                bobInputIOIndex: U256::from(1),
                bobOutputIOIndex: U256::from(0),
                aliceBountyVaultId: B256::ZERO,
                bobBountyVaultId: B256::ZERO,
            },
        }
    }

    fn create_after_clear_event() -> AfterClearV2 {
        AfterClearV2 {
            sender: address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            clearStateChange: ClearStateChangeV2 {
                aliceOutput: Float::from_fixed_decimal(uint!(9_U256), 0)
                    .unwrap()
                    .get_inner(),
                bobOutput: Float::from_fixed_decimal(uint!(100_U256), 0)
                    .unwrap()
                    .get_inner(),
                aliceInput: Float::from_fixed_decimal(uint!(100_U256), 0)
                    .unwrap()
                    .get_inner(),
                bobInput: Float::from_fixed_decimal(uint!(9_U256), 0)
                    .unwrap()
                    .get_inner(),
            },
        }
    }

    fn mocked_receipt_hex(tx_hash: alloy::primitives::FixedBytes<32>) -> serde_json::Value {
        json!({
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
    async fn test_try_from_clear_v3_alice_order_match() {
        let config = create_test_config();
        let cache = SymbolCache::default();

        let order = get_test_order();
        let different_order = {
            let mut order = get_test_order();
            order.nonce =
                fixed_bytes!("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
            order
        };

        let clear_event = create_clear_event(order.clone(), different_order);
        let orderbook = address!("0x1111111111111111111111111111111111111111");
        let tx_hash =
            fixed_bytes!("0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");

        let clear_log = Log {
            inner: alloy::primitives::Log {
                address: orderbook,
                data: clear_event.to_log_data(),
            },
            block_hash: None,
            block_number: Some(1),
            block_timestamp: None,
            transaction_hash: Some(tx_hash),
            transaction_index: None,
            log_index: Some(1),
            removed: false,
        };

        let after_clear_event = create_after_clear_event();
        let after_clear_log = Log {
            inner: alloy::primitives::Log {
                address: orderbook,
                data: after_clear_event.to_log_data(),
            },
            block_hash: None,
            block_number: Some(1),
            block_timestamp: None,
            transaction_hash: Some(tx_hash),
            transaction_index: None,
            log_index: Some(2), // Higher than clear log
            removed: false,
        };

        let asserter = Asserter::new();
        asserter.push_success(&json!([after_clear_log])); // get_logs returns AfterClearV2
        asserter.push_success(&mocked_receipt_hex(tx_hash)); // receipt for gas info
        // Mock decimals() then symbol() calls for input token (USDC)
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&6u8));
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"USDC".to_string(),
        ));
        // Mock decimals() then symbol() calls for output token (AAPL0x)
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&18u8));
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"AAPL0x".to_string(),
        ));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let feed_id_cache = FeedIdCache::default();

        let result = OnchainTrade::try_from_clear_v3(
            &config,
            &cache,
            provider,
            clear_event,
            clear_log,
            &feed_id_cache,
            config.order_owner.unwrap(),
        )
        .await
        .unwrap();

        let trade = result.unwrap();
        assert_eq!(trade.symbol, tokenized_symbol!("AAPL0x"));
        assert!((trade.amount - 9.0).abs() < f64::EPSILON);
        assert_eq!(trade.tx_hash, tx_hash);
        assert_eq!(trade.log_index, 1);
    }

    #[tokio::test]
    async fn test_try_from_clear_v3_bob_order_match() {
        let config = create_test_config();
        let cache = SymbolCache::default();

        let order = get_test_order();
        let different_order = {
            let mut order = get_test_order();
            order.owner = address!("0xffffffffffffffffffffffffffffffffffffffff");
            order
        };

        let clear_event = create_clear_event(different_order, order.clone());
        let orderbook = address!("0x1111111111111111111111111111111111111111");
        let tx_hash =
            fixed_bytes!("0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");

        let clear_log = Log {
            inner: alloy::primitives::Log {
                address: orderbook,
                data: clear_event.to_log_data(),
            },
            block_hash: None,
            block_number: Some(1),
            block_timestamp: None,
            transaction_hash: Some(tx_hash),
            transaction_index: None,
            log_index: Some(1),
            removed: false,
        };

        let after_clear_event = create_after_clear_event();
        let after_clear_log = Log {
            inner: alloy::primitives::Log {
                address: orderbook,
                data: after_clear_event.to_log_data(),
            },
            block_hash: None,
            block_number: Some(1),
            block_timestamp: None,
            transaction_hash: Some(tx_hash),
            transaction_index: None,
            log_index: Some(2), // Higher than clear log
            removed: false,
        };

        let asserter = Asserter::new();
        asserter.push_success(&json!([after_clear_log])); // get_logs returns AfterClearV2
        asserter.push_success(&mocked_receipt_hex(tx_hash)); // receipt for gas info
        // Mock decimals() then symbol() calls for input token (AAPL0x)
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&18u8));
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"AAPL0x".to_string(),
        ));
        // Mock decimals() then symbol() calls for output token (USDC)
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&6u8));
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"USDC".to_string(),
        ));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let feed_id_cache = FeedIdCache::default();

        let result = OnchainTrade::try_from_clear_v3(
            &config,
            &cache,
            provider,
            clear_event,
            clear_log,
            &feed_id_cache,
            config.order_owner.unwrap(),
        )
        .await
        .unwrap();

        let trade = result.unwrap();
        assert_eq!(trade.symbol, tokenized_symbol!("AAPL0x"));
        assert!((trade.amount - 9.0).abs() < f64::EPSILON);
        assert_eq!(trade.tx_hash, tx_hash);
        assert_eq!(trade.log_index, 1);
    }

    #[tokio::test]
    async fn test_try_from_clear_v3_no_order_match() {
        let config = create_test_config();
        let cache = SymbolCache::default();

        let different_order1 = {
            let mut order = get_test_order();
            order.owner = address!("0xffffffffffffffffffffffffffffffffffffffff");
            order
        };
        let different_order2 = {
            let mut order = get_test_order();
            order.owner = address!("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
            order
        };

        let clear_event = create_clear_event(different_order1, different_order2);
        let clear_log = get_test_log();

        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let feed_id_cache = FeedIdCache::default();

        let result = OnchainTrade::try_from_clear_v3(
            &config,
            &cache,
            provider,
            clear_event,
            clear_log,
            &feed_id_cache,
            config.order_owner.unwrap(),
        )
        .await
        .unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_try_from_clear_v3_missing_block_number() {
        let config = create_test_config();
        let cache = SymbolCache::default();

        let order = get_test_order();
        let different_order = {
            let mut order = get_test_order();
            order.nonce =
                fixed_bytes!("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
            order
        };

        let clear_event = create_clear_event(order.clone(), different_order);
        let mut clear_log = get_test_log();
        clear_log.block_number = None; // Missing block number

        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let feed_id_cache = FeedIdCache::default();

        let result = OnchainTrade::try_from_clear_v3(
            &config,
            &cache,
            provider,
            clear_event,
            clear_log,
            &feed_id_cache,
            config.order_owner.unwrap(),
        )
        .await;

        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(crate::error::TradeValidationError::NoBlockNumber)
        ));
    }

    #[tokio::test]
    async fn test_try_from_clear_v3_missing_after_clear_log() {
        let config = create_test_config();
        let cache = SymbolCache::default();

        let order = get_test_order();
        let different_order = {
            let mut order = get_test_order();
            order.nonce =
                fixed_bytes!("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
            order
        };

        let clear_event = create_clear_event(order.clone(), different_order);
        let clear_log = Log {
            inner: alloy::primitives::Log {
                address: address!("0x1111111111111111111111111111111111111111"),
                data: clear_event.to_log_data(),
            },
            block_hash: None,
            block_number: Some(1),
            block_timestamp: None,
            transaction_hash: Some(fixed_bytes!(
                "0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
            )),
            transaction_index: None,
            log_index: Some(1),
            removed: false,
        };

        let tx_hash =
            fixed_bytes!("0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
        let asserter = Asserter::new();
        asserter.push_success(&json!([])); // No after clear logs found
        asserter.push_success(&mocked_receipt_hex(tx_hash)); // Receipt with no logs
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let feed_id_cache = FeedIdCache::default();

        let result = OnchainTrade::try_from_clear_v3(
            &config,
            &cache,
            provider,
            clear_event,
            clear_log,
            &feed_id_cache,
            config.order_owner.unwrap(),
        )
        .await;

        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(TradeValidationError::NodeReceiptMissing { .. })
        ));
    }

    #[tokio::test]
    async fn test_try_from_clear_v3_after_clear_wrong_transaction() {
        let config = create_test_config();
        let cache = SymbolCache::default();

        let order = get_test_order();
        let different_order = {
            let mut order = get_test_order();
            order.nonce =
                fixed_bytes!("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
            order
        };

        let clear_event = create_clear_event(order.clone(), different_order);
        let orderbook = address!("0x1111111111111111111111111111111111111111");
        let tx_hash =
            fixed_bytes!("0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");

        let clear_log = Log {
            inner: alloy::primitives::Log {
                address: orderbook,
                data: clear_event.to_log_data(),
            },
            block_hash: None,
            block_number: Some(1),
            block_timestamp: None,
            transaction_hash: Some(tx_hash),
            transaction_index: None,
            log_index: Some(1),
            removed: false,
        };

        let after_clear_event = create_after_clear_event();
        let wrong_after_clear_log = Log {
            inner: alloy::primitives::Log {
                address: orderbook,
                data: after_clear_event.to_log_data(),
            },
            block_hash: None,
            block_number: Some(1),
            block_timestamp: None,
            transaction_hash: Some(fixed_bytes!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            )), // Different tx hash
            transaction_index: None,
            log_index: Some(2),
            removed: false,
        };

        let asserter = Asserter::new();
        asserter.push_success(&json!([wrong_after_clear_log])); // Wrong transaction hash
        asserter.push_success(&mocked_receipt_hex(tx_hash)); // Receipt with no logs
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let feed_id_cache = FeedIdCache::default();

        let result = OnchainTrade::try_from_clear_v3(
            &config,
            &cache,
            provider,
            clear_event,
            clear_log,
            &feed_id_cache,
            config.order_owner.unwrap(),
        )
        .await;

        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(TradeValidationError::NodeReceiptMissing { .. })
        ));
    }

    #[tokio::test]
    async fn test_try_from_clear_v3_after_clear_wrong_log_index() {
        let config = create_test_config();
        let cache = SymbolCache::default();

        let order = get_test_order();
        let different_order = {
            let mut order = get_test_order();
            order.nonce =
                fixed_bytes!("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
            order
        };

        let clear_event = create_clear_event(order.clone(), different_order);
        let orderbook = address!("0x1111111111111111111111111111111111111111");
        let tx_hash =
            fixed_bytes!("0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");

        let clear_log = Log {
            inner: alloy::primitives::Log {
                address: orderbook,
                data: clear_event.to_log_data(),
            },
            block_hash: None,
            block_number: Some(1),
            block_timestamp: None,
            transaction_hash: Some(tx_hash),
            transaction_index: None,
            log_index: Some(5), // Higher log index
            removed: false,
        };

        let after_clear_event = create_after_clear_event();
        let wrong_after_clear_log = Log {
            inner: alloy::primitives::Log {
                address: orderbook,
                data: after_clear_event.to_log_data(),
            },
            block_hash: None,
            block_number: Some(1),
            block_timestamp: None,
            transaction_hash: Some(tx_hash),
            transaction_index: None,
            log_index: Some(2), // Lower than clear log index
            removed: false,
        };

        let asserter = Asserter::new();
        asserter.push_success(&json!([wrong_after_clear_log])); // Wrong log index ordering
        asserter.push_success(&mocked_receipt_hex(tx_hash)); // Receipt with no logs
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let feed_id_cache = FeedIdCache::default();

        let result = OnchainTrade::try_from_clear_v3(
            &config,
            &cache,
            provider,
            clear_event,
            clear_log,
            &feed_id_cache,
            config.order_owner.unwrap(),
        )
        .await;

        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(TradeValidationError::NodeReceiptMissing { .. })
        ));
    }

    #[tokio::test]
    async fn test_try_from_clear_v3_alice_and_bob_both_match() {
        let config = create_test_config();
        let cache = SymbolCache::default();

        let order = get_test_order();

        // Both Alice and Bob have the target order hash
        let clear_event = create_clear_event(order.clone(), order.clone());
        let orderbook = address!("0x1111111111111111111111111111111111111111");
        let tx_hash =
            fixed_bytes!("0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");

        let clear_log = Log {
            inner: alloy::primitives::Log {
                address: orderbook,
                data: clear_event.to_log_data(),
            },
            block_hash: None,
            block_number: Some(1),
            block_timestamp: None,
            transaction_hash: Some(tx_hash),
            transaction_index: None,
            log_index: Some(1),
            removed: false,
        };

        let after_clear_event = create_after_clear_event();
        let after_clear_log = Log {
            inner: alloy::primitives::Log {
                address: orderbook,
                data: after_clear_event.to_log_data(),
            },
            block_hash: None,
            block_number: Some(1),
            block_timestamp: None,
            transaction_hash: Some(tx_hash),
            transaction_index: None,
            log_index: Some(2),
            removed: false,
        };

        let asserter = Asserter::new();
        asserter.push_success(&json!([after_clear_log])); // get_logs returns AfterClearV2
        asserter.push_success(&mocked_receipt_hex(tx_hash)); // receipt for gas info
        // Mock decimals() then symbol() calls for input token (USDC)
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&6u8));
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"USDC".to_string(),
        ));
        // Mock decimals() then symbol() calls for output token (AAPL0x)
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&18u8));
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"AAPL0x".to_string(),
        ));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let feed_id_cache = FeedIdCache::default();

        let result = OnchainTrade::try_from_clear_v3(
            &config,
            &cache,
            provider,
            clear_event,
            clear_log,
            &feed_id_cache,
            config.order_owner.unwrap(),
        )
        .await
        .unwrap();

        // Should process Alice first (alice_hash_matches is checked first)
        let trade = result.unwrap();
        assert_eq!(trade.symbol, tokenized_symbol!("AAPL0x"));
        assert!((trade.amount - 9.0).abs() < f64::EPSILON);
        assert_eq!(trade.tx_hash, tx_hash);
        assert_eq!(trade.log_index, 1);
    }

    fn create_parameterized_after_clear_event(
        sender_byte: u8,
        alice_shares: u64,
        bob_usdc: u64,
    ) -> AfterClearV2 {
        AfterClearV2 {
            sender: Address::repeat_byte(sender_byte),
            clearStateChange: ClearStateChangeV2 {
                aliceOutput: Float::from_fixed_decimal(U256::from(alice_shares), 0)
                    .unwrap()
                    .get_inner(),
                bobOutput: Float::from_fixed_decimal(U256::from(bob_usdc), 0)
                    .unwrap()
                    .get_inner(),
                aliceInput: Float::from_fixed_decimal(U256::from(bob_usdc), 0)
                    .unwrap()
                    .get_inner(),
                bobInput: Float::from_fixed_decimal(U256::from(alice_shares), 0)
                    .unwrap()
                    .get_inner(),
            },
        }
    }

    fn create_test_log(
        orderbook: Address,
        tx_hash: B256,
        log_data: LogData,
        log_index: u64,
    ) -> Log {
        Log {
            inner: alloy::primitives::Log {
                address: orderbook,
                data: log_data,
            },
            block_hash: None,
            block_number: Some(1),
            block_timestamp: None,
            transaction_hash: Some(tx_hash),
            transaction_index: None,
            log_index: Some(log_index),
            removed: false,
        }
    }

    fn create_receipt_json_with_logs(
        tx_hash: B256,
        logs: &[serde_json::Value],
    ) -> serde_json::Value {
        json!({
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
            "logs": logs
        })
    }

    fn create_receipt_log_json(
        address: Address,
        tx_hash: B256,
        log_index: u64,
        topics: &[B256],
        data: Bytes,
    ) -> serde_json::Value {
        let topics_hex: Vec<String> = topics.iter().map(hex::encode_prefixed).collect();
        json!({
            "address": hex::encode_prefixed(address),
            "topics": topics_hex,
            "data": hex::encode_prefixed(data),
            "blockHash": "0x1234567890123456789012345678901234567890123456789012345678901234",
            "blockNumber": "0x1",
            "transactionHash": hex::encode_prefixed(tx_hash),
            "transactionIndex": "0x1",
            "logIndex": format!("0x{:x}", log_index),
            "removed": false
        })
    }

    #[tokio::test]
    async fn test_fetch_after_clear_multiple_logs_picks_first_match() {
        let config = create_test_config();
        let cache = SymbolCache::default();

        let order = get_test_order();
        let different_order = {
            let mut order = get_test_order();
            order.nonce =
                fixed_bytes!("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
            order
        };

        let clear_event = create_clear_event(order.clone(), different_order);
        let orderbook = address!("0x1111111111111111111111111111111111111111");
        let tx_hash =
            fixed_bytes!("0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");

        let clear_log = create_test_log(orderbook, tx_hash, clear_event.to_log_data(), 5);

        let after_clear_event_1 = create_parameterized_after_clear_event(0xaa, 9, 100);
        let after_clear_event_2 = create_parameterized_after_clear_event(0xbb, 5, 50);

        let after_clear_log_1 =
            create_test_log(orderbook, tx_hash, after_clear_event_1.to_log_data(), 6);
        let after_clear_log_2 =
            create_test_log(orderbook, tx_hash, after_clear_event_2.to_log_data(), 7);

        let asserter = Asserter::new();
        asserter.push_success(&json!([after_clear_log_1, after_clear_log_2])); // get_logs returns multiple AfterClearV2
        let receipt_json = create_receipt_json_with_logs(tx_hash, &[]);
        asserter.push_success(&receipt_json); // receipt for gas info
        // Mock decimals() then symbol() calls for input token (USDC)
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&6u8));
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"USDC".to_string(),
        ));
        // Mock decimals() then symbol() calls for output token (AAPL0x)
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&18u8));
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"AAPL0x".to_string(),
        ));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let feed_id_cache = FeedIdCache::default();

        let result = OnchainTrade::try_from_clear_v3(
            &config,
            &cache,
            provider,
            clear_event,
            clear_log,
            &feed_id_cache,
            config.order_owner.unwrap(),
        )
        .await
        .unwrap();

        let trade = result.unwrap();
        assert_eq!(trade.symbol, tokenized_symbol!("AAPL0x"));
        assert!((trade.amount - 9.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_fetch_after_clear_equal_log_index_rejected() {
        let config = create_test_config();
        let cache = SymbolCache::default();

        let order = get_test_order();
        let different_order = {
            let mut order = get_test_order();
            order.nonce =
                fixed_bytes!("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
            order
        };

        let clear_event = create_clear_event(order.clone(), different_order);
        let orderbook = address!("0x1111111111111111111111111111111111111111");
        let tx_hash =
            fixed_bytes!("0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");

        let clear_log = Log {
            inner: alloy::primitives::Log {
                address: orderbook,
                data: clear_event.to_log_data(),
            },
            block_hash: None,
            block_number: Some(1),
            block_timestamp: None,
            transaction_hash: Some(tx_hash),
            transaction_index: None,
            log_index: Some(5),
            removed: false,
        };

        let after_clear_event = create_after_clear_event();
        let after_clear_log_equal_index = Log {
            inner: alloy::primitives::Log {
                address: orderbook,
                data: after_clear_event.to_log_data(),
            },
            block_hash: None,
            block_number: Some(1),
            block_timestamp: None,
            transaction_hash: Some(tx_hash),
            transaction_index: None,
            log_index: Some(5),
            removed: false,
        };

        let asserter = Asserter::new();
        asserter.push_success(&json!([after_clear_log_equal_index])); // get_logs returns log with equal index (not valid)
        asserter.push_success(&mocked_receipt_hex(tx_hash)); // receipt has no logs
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let feed_id_cache = FeedIdCache::default();

        let result = OnchainTrade::try_from_clear_v3(
            &config,
            &cache,
            provider,
            clear_event,
            clear_log,
            &feed_id_cache,
            config.order_owner.unwrap(),
        )
        .await;

        // Falls back to receipt, but receipt has no logs, so NodeReceiptMissing
        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(TradeValidationError::NodeReceiptMissing { .. })
        ));
    }

    #[tokio::test]
    async fn test_fetch_after_clear_mixed_transactions_finds_correct() {
        let config = create_test_config();
        let cache = SymbolCache::default();

        let order = get_test_order();
        let different_order = {
            let mut order = get_test_order();
            order.nonce =
                fixed_bytes!("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
            order
        };

        let clear_event = create_clear_event(order.clone(), different_order);
        let orderbook = address!("0x1111111111111111111111111111111111111111");
        let target_tx_hash =
            fixed_bytes!("0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
        let other_tx_hash =
            fixed_bytes!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

        let clear_log = create_test_log(orderbook, target_tx_hash, clear_event.to_log_data(), 3);

        let after_clear_event_correct = create_after_clear_event();
        let after_clear_event_wrong = create_parameterized_after_clear_event(0xcc, 1, 10);

        let wrong_tx_log = create_test_log(
            orderbook,
            other_tx_hash,
            after_clear_event_wrong.to_log_data(),
            4,
        );
        let correct_log = create_test_log(
            orderbook,
            target_tx_hash,
            after_clear_event_correct.to_log_data(),
            5,
        );

        let asserter = Asserter::new();
        // get_logs returns a log with wrong tx_hash but also the correct one
        asserter.push_success(&json!([wrong_tx_log, correct_log]));
        // Receipt mock needed for gas info in try_from_order_and_fill_details (empty is fine)
        let receipt_json = create_receipt_json_with_logs(target_tx_hash, &[]);
        asserter.push_success(&receipt_json);
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

        let result = OnchainTrade::try_from_clear_v3(
            &config,
            &cache,
            provider,
            clear_event,
            clear_log,
            &feed_id_cache,
            config.order_owner.unwrap(),
        )
        .await
        .unwrap();

        let trade = result.unwrap();
        assert_eq!(trade.symbol, tokenized_symbol!("AAPL0x"));
        assert!((trade.amount - 9.0).abs() < f64::EPSILON);
    }

    fn create_after_clear_log_data() -> (Vec<B256>, Bytes) {
        let log_data = create_after_clear_event().to_log_data();
        (log_data.topics().to_vec(), log_data.data)
    }

    fn create_clear_v3_log_data(event: &ClearV3) -> (Vec<B256>, Bytes) {
        let log_data = event.to_log_data();
        (log_data.topics().to_vec(), log_data.data)
    }

    #[tokio::test]
    async fn test_fallback_to_receipt_when_get_logs_returns_empty() {
        let config = create_test_config();
        let cache = SymbolCache::default();

        let order = get_test_order();
        let different_order = {
            let mut order = get_test_order();
            order.nonce =
                fixed_bytes!("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
            order
        };

        let clear_event = create_clear_event(order.clone(), different_order);
        let orderbook = address!("0x1111111111111111111111111111111111111111");
        let tx_hash =
            fixed_bytes!("0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");

        let clear_log = create_test_log(orderbook, tx_hash, clear_event.to_log_data(), 1);

        let (after_clear_topics, after_clear_data) = create_after_clear_log_data();
        let receipt_log =
            create_receipt_log_json(orderbook, tx_hash, 2, &after_clear_topics, after_clear_data);
        let receipt = create_receipt_json_with_logs(tx_hash, &[receipt_log]);

        let asserter = Asserter::new();
        asserter.push_success(&json!([])); // get_logs returns empty (simulating unreliable node)
        asserter.push_success(&receipt); // receipt for fallback AfterClearV2 extraction
        asserter.push_success(&receipt); // receipt for gas info in try_from_order_and_fill_details
        // Mock decimals() then symbol() calls for input token (USDC)
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&6u8));
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"USDC".to_string(),
        ));
        // Mock decimals() then symbol() calls for output token (AAPL0x)
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&18u8));
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"AAPL0x".to_string(),
        ));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let feed_id_cache = FeedIdCache::default();

        let result = OnchainTrade::try_from_clear_v3(
            &config,
            &cache,
            provider,
            clear_event,
            clear_log,
            &feed_id_cache,
            config.order_owner.unwrap(),
        )
        .await
        .unwrap();

        let trade = result.unwrap();
        assert_eq!(trade.symbol, tokenized_symbol!("AAPL0x"));
        assert!((trade.amount - 9.0).abs() < f64::EPSILON);
        assert_eq!(trade.tx_hash, tx_hash);
        assert_eq!(trade.log_index, 1);
    }

    #[tokio::test]
    async fn test_fallback_receipt_with_multiple_logs_picks_correct_after_clear() {
        let config = create_test_config();
        let cache = SymbolCache::default();

        let order = get_test_order();
        let different_order = {
            let mut order = get_test_order();
            order.nonce =
                fixed_bytes!("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
            order
        };

        let clear_event = create_clear_event(order.clone(), different_order);
        let orderbook = address!("0x1111111111111111111111111111111111111111");
        let tx_hash =
            fixed_bytes!("0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");

        let clear_log = create_test_log(orderbook, tx_hash, clear_event.to_log_data(), 3);

        // Create two AfterClearV2 logs in receipt - only the one with log_index > 3 should be used
        let (after_clear_topics, after_clear_data) = create_after_clear_log_data();
        let wrong_receipt_log = create_receipt_log_json(
            orderbook,
            tx_hash,
            2, // log_index 2 <= 3, should be skipped
            &after_clear_topics,
            after_clear_data.clone(),
        );
        let correct_receipt_log = create_receipt_log_json(
            orderbook,
            tx_hash,
            4, // log_index 4 > 3, should be used
            &after_clear_topics,
            after_clear_data,
        );
        let receipt =
            create_receipt_json_with_logs(tx_hash, &[wrong_receipt_log, correct_receipt_log]);

        let asserter = Asserter::new();
        asserter.push_success(&json!([])); // get_logs returns empty
        asserter.push_success(&receipt); // receipt for fallback AfterClearV2 extraction
        asserter.push_success(&receipt); // receipt for gas info in try_from_order_and_fill_details
        // Mock decimals() then symbol() calls for input token (USDC)
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&6u8));
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"USDC".to_string(),
        ));
        // Mock decimals() then symbol() calls for output token (AAPL0x)
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&18u8));
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"AAPL0x".to_string(),
        ));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let feed_id_cache = FeedIdCache::default();

        let result = OnchainTrade::try_from_clear_v3(
            &config,
            &cache,
            provider,
            clear_event,
            clear_log,
            &feed_id_cache,
            config.order_owner.unwrap(),
        )
        .await
        .unwrap();

        let trade = result.unwrap();
        assert_eq!(trade.symbol, tokenized_symbol!("AAPL0x"));
        assert!((trade.amount - 9.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_fallback_receipt_rejects_wrong_contract_address() {
        let config = create_test_config();
        let cache = SymbolCache::default();

        let order = get_test_order();
        let different_order = {
            let mut order = get_test_order();
            order.nonce =
                fixed_bytes!("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
            order
        };

        let clear_event = create_clear_event(order.clone(), different_order);
        let orderbook = address!("0x1111111111111111111111111111111111111111");
        let wrong_address = address!("0x2222222222222222222222222222222222222222");
        let tx_hash =
            fixed_bytes!("0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");

        let clear_log = create_test_log(orderbook, tx_hash, clear_event.to_log_data(), 1);

        let (after_clear_topics, after_clear_data) = create_after_clear_log_data();
        let wrong_address_log = create_receipt_log_json(
            wrong_address, // Wrong contract address
            tx_hash,
            2,
            &after_clear_topics,
            after_clear_data,
        );
        let receipt = create_receipt_json_with_logs(tx_hash, &[wrong_address_log]);

        let asserter = Asserter::new();
        asserter.push_success(&json!([])); // get_logs returns empty (simulating unreliable node)
        asserter.push_success(&receipt); // receipt for fallback - has log but wrong contract address
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let feed_id_cache = FeedIdCache::default();

        let result = OnchainTrade::try_from_clear_v3(
            &config,
            &cache,
            provider,
            clear_event,
            clear_log,
            &feed_id_cache,
            config.order_owner.unwrap(),
        )
        .await;

        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(TradeValidationError::NodeReceiptMissing { .. })
        ));
    }

    #[tokio::test]
    async fn test_fallback_receipt_rejects_log_index_less_than_or_equal_to_clear() {
        let config = create_test_config();
        let cache = SymbolCache::default();

        let order = get_test_order();
        let different_order = {
            let mut order = get_test_order();
            order.nonce =
                fixed_bytes!("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
            order
        };

        let clear_event = create_clear_event(order.clone(), different_order);
        let orderbook = address!("0x1111111111111111111111111111111111111111");
        let tx_hash =
            fixed_bytes!("0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");

        let clear_log = create_test_log(orderbook, tx_hash, clear_event.to_log_data(), 5);

        let (after_clear_topics, after_clear_data) = create_after_clear_log_data();
        let earlier_log = create_receipt_log_json(
            orderbook,
            tx_hash,
            3, // log_index 3 < 5, should be rejected
            &after_clear_topics,
            after_clear_data,
        );
        let receipt = create_receipt_json_with_logs(tx_hash, &[earlier_log]);

        let asserter = Asserter::new();
        asserter.push_success(&json!([])); // get_logs returns empty (simulating unreliable node)
        asserter.push_success(&receipt); // receipt for fallback - has AfterClearV2 but log_index < clear's
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let feed_id_cache = FeedIdCache::default();

        let result = OnchainTrade::try_from_clear_v3(
            &config,
            &cache,
            provider,
            clear_event,
            clear_log,
            &feed_id_cache,
            config.order_owner.unwrap(),
        )
        .await;

        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(TradeValidationError::NodeReceiptMissing { .. })
        ));
    }

    #[tokio::test]
    async fn test_fallback_after_clear_missing_from_receipt_error() {
        let config = create_test_config();
        let cache = SymbolCache::default();

        let order = get_test_order();
        let different_order = {
            let mut order = get_test_order();
            order.nonce =
                fixed_bytes!("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
            order
        };

        let clear_event = create_clear_event(order.clone(), different_order);
        let orderbook = address!("0x1111111111111111111111111111111111111111");
        let tx_hash =
            fixed_bytes!("0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");

        let clear_log = create_test_log(orderbook, tx_hash, clear_event.to_log_data(), 1);

        // Receipt has ClearV3 but no AfterClearV2 - theoretically impossible but we handle it
        let (clear_topics, clear_data) = create_clear_v3_log_data(&clear_event);
        let clear_log_in_receipt =
            create_receipt_log_json(orderbook, tx_hash, 1, &clear_topics, clear_data);
        let receipt = create_receipt_json_with_logs(tx_hash, &[clear_log_in_receipt]);

        let asserter = Asserter::new();
        asserter.push_success(&json!([])); // get_logs returns empty (simulating unreliable node)
        asserter.push_success(&receipt); // receipt for fallback - has ClearV3 but no AfterClearV2
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let feed_id_cache = FeedIdCache::default();

        let result = OnchainTrade::try_from_clear_v3(
            &config,
            &cache,
            provider,
            clear_event,
            clear_log,
            &feed_id_cache,
            config.order_owner.unwrap(),
        )
        .await;

        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(TradeValidationError::AfterClearMissingFromReceipt { .. })
        ));
    }

    #[tokio::test]
    async fn test_fallback_receipt_ignores_wrong_event_signature() {
        let config = create_test_config();
        let cache = SymbolCache::default();

        let order = get_test_order();
        let different_order = {
            let mut order = get_test_order();
            order.nonce =
                fixed_bytes!("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
            order
        };

        let clear_event = create_clear_event(order.clone(), different_order);
        let orderbook = address!("0x1111111111111111111111111111111111111111");
        let tx_hash =
            fixed_bytes!("0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");

        let clear_log = create_test_log(orderbook, tx_hash, clear_event.to_log_data(), 1);

        // Receipt has a log with correct address but unrelated event signature (random hash)
        let (_, after_clear_data) = create_after_clear_log_data();
        let unrelated_event_sig =
            fixed_bytes!("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef");
        let wrong_sig_log = create_receipt_log_json(
            orderbook,
            tx_hash,
            2,
            &[unrelated_event_sig], // Unrelated event signature (not ClearV3 or AfterClearV2)
            after_clear_data,
        );
        let receipt = create_receipt_json_with_logs(tx_hash, &[wrong_sig_log]);

        let asserter = Asserter::new();
        asserter.push_success(&json!([])); // get_logs returns empty (simulating unreliable node)
        asserter.push_success(&receipt); // receipt for fallback - has log but unrecognized event signature
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let feed_id_cache = FeedIdCache::default();

        let result = OnchainTrade::try_from_clear_v3(
            &config,
            &cache,
            provider,
            clear_event,
            clear_log,
            &feed_id_cache,
            config.order_owner.unwrap(),
        )
        .await;

        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(TradeValidationError::NodeReceiptMissing { .. })
        ));
    }

    #[tokio::test]
    async fn test_fallback_get_logs_has_wrong_tx_but_receipt_has_correct() {
        let config = create_test_config();
        let cache = SymbolCache::default();

        let order = get_test_order();
        let different_order = {
            let mut order = get_test_order();
            order.nonce =
                fixed_bytes!("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
            order
        };

        let clear_event = create_clear_event(order.clone(), different_order);
        let orderbook = address!("0x1111111111111111111111111111111111111111");
        let target_tx_hash =
            fixed_bytes!("0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
        let other_tx_hash =
            fixed_bytes!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

        let clear_log = create_test_log(orderbook, target_tx_hash, clear_event.to_log_data(), 1);

        // get_logs returns a log with wrong tx_hash
        let after_clear_event = create_after_clear_event();
        let wrong_tx_log =
            create_test_log(orderbook, other_tx_hash, after_clear_event.to_log_data(), 2);

        // But receipt has the correct log
        let (after_clear_topics, after_clear_data) = create_after_clear_log_data();
        let correct_receipt_log = create_receipt_log_json(
            orderbook,
            target_tx_hash,
            2,
            &after_clear_topics,
            after_clear_data,
        );
        let receipt = create_receipt_json_with_logs(target_tx_hash, &[correct_receipt_log]);

        let asserter = Asserter::new();
        asserter.push_success(&json!([wrong_tx_log])); // get_logs returns wrong tx
        asserter.push_success(&receipt); // receipt for fallback AfterClearV2 extraction
        asserter.push_success(&receipt); // receipt for gas info in try_from_order_and_fill_details
        // Mock decimals() then symbol() calls for input token (USDC)
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&6u8));
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"USDC".to_string(),
        ));
        // Mock decimals() then symbol() calls for output token (AAPL0x)
        asserter.push_success(&<decimalsCall as SolCall>::abi_encode_returns(&18u8));
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"AAPL0x".to_string(),
        ));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let feed_id_cache = FeedIdCache::default();

        let result = OnchainTrade::try_from_clear_v3(
            &config,
            &cache,
            provider,
            clear_event,
            clear_log,
            &feed_id_cache,
            config.order_owner.unwrap(),
        )
        .await
        .unwrap();

        let trade = result.unwrap();
        assert_eq!(trade.symbol, tokenized_symbol!("AAPL0x"));
        assert!((trade.amount - 9.0).abs() < f64::EPSILON);
    }
}

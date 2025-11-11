use alloy::providers::Provider;
use alloy::rpc::types::{Filter, Log};
use alloy::sol_types::SolEvent;
use tracing::{debug, info};

use crate::bindings::IOrderBookV5::{AfterClearV2, ClearConfigV2, ClearStateChangeV2, ClearV3};
use crate::error::{OnChainError, TradeValidationError};
use crate::onchain::{
    EvmEnv,
    pyth::FeedIdCache,
    trade::{OnchainTrade, OrderFill},
};
use crate::symbol::cache::SymbolCache;

impl OnchainTrade {
    /// Creates OnchainTrade directly from ClearV3 blockchain events
    #[tracing::instrument(skip_all, fields(tx_hash = ?log.transaction_hash, log_index = ?log.log_index), level = tracing::Level::DEBUG)]
    pub async fn try_from_clear_v3<P: Provider>(
        env: &EvmEnv,
        cache: &SymbolCache,
        provider: P,
        event: ClearV3,
        log: Log,
        feed_id_cache: &FeedIdCache,
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

        let alice_owner_matches = alice_order.owner == env.order_owner;
        let bob_owner_matches = bob_order.owner == env.order_owner;

        debug!(
            "ClearV3 owner comparison: alice.owner={:?}, bob.owner={:?}, env.order_owner={:?}, alice_matches={}, bob_matches={}",
            alice_order.owner,
            bob_order.owner,
            env.order_owner,
            alice_owner_matches,
            bob_owner_matches
        );

        if !(alice_owner_matches || bob_owner_matches) {
            info!(
                "ClearV3 event filtered (no owner match): tx_hash={:?}, log_index={}, alice.owner={:?}, bob.owner={:?}, target={:?}",
                log.transaction_hash,
                log.log_index.unwrap_or(0),
                alice_order.owner,
                bob_order.owner,
                env.order_owner
            );
            return Ok(None);
        }

        let after_clear = fetch_after_clear_event(&provider, env, &log).await?;

        let ClearStateChangeV2 {
            aliceOutput,
            bobOutput,
            aliceInput,
            bobInput,
        } = after_clear.clearStateChange;

        let (order, fill) = if alice_owner_matches {
            let fill = OrderFill {
                input_index: usize::try_from(aliceInputIOIndex)?,
                input_amount: aliceInput.into(),
                output_index: usize::try_from(aliceOutputIOIndex)?,
                output_amount: aliceOutput.into(),
            };
            (alice_order, fill)
        } else {
            let fill = OrderFill {
                input_index: usize::try_from(bobInputIOIndex)?,
                input_amount: bobInput.into(),
                output_index: usize::try_from(bobOutputIOIndex)?,
                output_amount: bobOutput.into(),
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

async fn fetch_after_clear_event<P: Provider>(
    provider: &P,
    env: &EvmEnv,
    log: &Log,
) -> Result<AfterClearV2, OnChainError> {
    let block_number = log
        .block_number
        .ok_or(TradeValidationError::NoBlockNumber)?;

    let filter = Filter::new()
        .select(block_number)
        .address(env.orderbook)
        .event_signature(AfterClearV2::SIGNATURE_HASH);

    let after_clear_logs = provider.get_logs(&filter).await?;
    let after_clear_log = after_clear_logs
        .iter()
        .find(|after_clear_log| {
            after_clear_log.transaction_hash == log.transaction_hash
                && after_clear_log.log_index > log.log_index
        })
        .ok_or(TradeValidationError::NoAfterClearLog)?;

    Ok(after_clear_log.log_decode::<AfterClearV2>()?.data().clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bindings::IERC20::symbolCall;
    use crate::bindings::IOrderBookV5::{AfterClearV2, ClearConfigV2, ClearStateChangeV2, ClearV3};
    use crate::onchain::pyth::FeedIdCache;
    use crate::symbol::cache::SymbolCache;
    use crate::test_utils::{get_test_log, get_test_order_v4};
    use crate::tokenized_symbol;
    use alloy::hex;
    use alloy::primitives::{IntoLogData, U256, address, fixed_bytes};
    use alloy::providers::{ProviderBuilder, mock::Asserter};
    use alloy::rpc::types::Log;
    use alloy::sol_types::SolCall;
    use serde_json::json;
    use std::str::FromStr;

    fn create_test_env() -> EvmEnv {
        EvmEnv {
            ws_rpc_url: url::Url::parse("ws://localhost:8545").unwrap(),
            orderbook: address!("0x1111111111111111111111111111111111111111"),
            order_owner: get_test_order_v4().owner,
            deployment_block: 1,
        }
    }

    fn create_clear_event(
        alice_order: crate::bindings::IOrderBookV5::OrderV4,
        bob_order: crate::bindings::IOrderBookV5::OrderV4,
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
                aliceBountyVaultId: [0u8; 32].into(),
                bobBountyVaultId: [0u8; 32].into(),
            },
        }
    }

    fn create_after_clear_event() -> AfterClearV2 {
        AfterClearV2 {
            sender: address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            clearStateChange: ClearStateChangeV2 {
                aliceOutput: U256::from_str("9000000000000000000").unwrap().into(), // 9 shares (18 dps)
                bobOutput: U256::from(100_000_000u64).into(), // 100 USDC (6 dps)
                aliceInput: U256::from(100_000_000u64).into(), // 100 USDC (6 dps)
                bobInput: U256::from_str("9000000000000000000").unwrap().into(), // 9 shares (18 dps)
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
    async fn test_try_from_clear_v2_alice_order_match() {
        let env = create_test_env();
        let cache = SymbolCache::default();

        let order = get_test_order_v4();
        let different_order = {
            let mut order = get_test_order_v4();
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
        asserter.push_success(&json!([after_clear_log]));
        asserter.push_success(&mocked_receipt_hex(tx_hash));
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"USDC".to_string(),
        ));
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"AAPL0x".to_string(),
        ));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let feed_id_cache = FeedIdCache::default();

        let result = OnchainTrade::try_from_clear_v3(
            &env,
            &cache,
            provider,
            clear_event,
            clear_log,
            &feed_id_cache,
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
    async fn test_try_from_clear_v2_bob_order_match() {
        let env = create_test_env();
        let cache = SymbolCache::default();

        let order = get_test_order_v4();
        let different_order = {
            let mut order = get_test_order_v4();
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
        asserter.push_success(&json!([after_clear_log]));
        asserter.push_success(&mocked_receipt_hex(tx_hash));
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"AAPL0x".to_string(),
        ));
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"USDC".to_string(),
        ));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let feed_id_cache = FeedIdCache::default();

        let result = OnchainTrade::try_from_clear_v3(
            &env,
            &cache,
            provider,
            clear_event,
            clear_log,
            &feed_id_cache,
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
    async fn test_try_from_clear_v2_no_order_match() {
        let env = create_test_env();
        let cache = SymbolCache::default();

        let different_order1 = {
            let mut order = get_test_order_v4();
            order.owner = address!("0xffffffffffffffffffffffffffffffffffffffff");
            order
        };
        let different_order2 = {
            let mut order = get_test_order_v4();
            order.owner = address!("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
            order
        };

        let clear_event = create_clear_event(different_order1, different_order2);
        let clear_log = get_test_log();

        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let feed_id_cache = FeedIdCache::default();

        let result = OnchainTrade::try_from_clear_v3(
            &env,
            &cache,
            provider,
            clear_event,
            clear_log,
            &feed_id_cache,
        )
        .await
        .unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_try_from_clear_v2_missing_block_number() {
        let env = create_test_env();
        let cache = SymbolCache::default();

        let order = get_test_order_v4();
        let different_order = {
            let mut order = get_test_order_v4();
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
            &env,
            &cache,
            provider,
            clear_event,
            clear_log,
            &feed_id_cache,
        )
        .await;

        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(crate::error::TradeValidationError::NoBlockNumber)
        ));
    }

    #[tokio::test]
    async fn test_try_from_clear_v2_missing_after_clear_log() {
        let env = create_test_env();
        let cache = SymbolCache::default();

        let order = get_test_order_v4();
        let different_order = {
            let mut order = get_test_order_v4();
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

        let asserter = Asserter::new();
        asserter.push_success(&json!([])); // No after clear logs found
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let feed_id_cache = FeedIdCache::default();

        let result = OnchainTrade::try_from_clear_v3(
            &env,
            &cache,
            provider,
            clear_event,
            clear_log,
            &feed_id_cache,
        )
        .await;

        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(crate::error::TradeValidationError::NoAfterClearLog)
        ));
    }

    #[tokio::test]
    async fn test_try_from_clear_v2_after_clear_wrong_transaction() {
        let env = create_test_env();
        let cache = SymbolCache::default();

        let order = get_test_order_v4();
        let different_order = {
            let mut order = get_test_order_v4();
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
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let feed_id_cache = FeedIdCache::default();

        let result = OnchainTrade::try_from_clear_v3(
            &env,
            &cache,
            provider,
            clear_event,
            clear_log,
            &feed_id_cache,
        )
        .await;

        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(crate::error::TradeValidationError::NoAfterClearLog)
        ));
    }

    #[tokio::test]
    async fn test_try_from_clear_v2_after_clear_wrong_log_index() {
        let env = create_test_env();
        let cache = SymbolCache::default();

        let order = get_test_order_v4();
        let different_order = {
            let mut order = get_test_order_v4();
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
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let feed_id_cache = FeedIdCache::default();

        let result = OnchainTrade::try_from_clear_v3(
            &env,
            &cache,
            provider,
            clear_event,
            clear_log,
            &feed_id_cache,
        )
        .await;

        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(crate::error::TradeValidationError::NoAfterClearLog)
        ));
    }

    #[tokio::test]
    async fn test_try_from_clear_v2_alice_and_bob_both_match() {
        let env = create_test_env();
        let cache = SymbolCache::default();

        let order = get_test_order_v4();

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
        asserter.push_success(&json!([after_clear_log]));
        asserter.push_success(&mocked_receipt_hex(tx_hash));
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"USDC".to_string(),
        ));
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"AAPL0x".to_string(),
        ));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let feed_id_cache = FeedIdCache::default();

        let result = OnchainTrade::try_from_clear_v3(
            &env,
            &cache,
            provider,
            clear_event,
            clear_log,
            &feed_id_cache,
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
        alice_output: &str,
        bob_output: u64,
    ) -> AfterClearV2 {
        AfterClearV2 {
            sender: alloy::primitives::Address::repeat_byte(sender_byte),
            clearStateChange: ClearStateChangeV2 {
                aliceOutput: U256::from_str(alice_output).unwrap().into(),
                bobOutput: U256::from(bob_output).into(),
                aliceInput: U256::from(bob_output).into(),
                bobInput: U256::from_str(alice_output).unwrap().into(),
            },
        }
    }

    fn create_test_log(
        orderbook: alloy::primitives::Address,
        tx_hash: alloy::primitives::B256,
        log_data: alloy::primitives::LogData,
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

    fn create_test_receipt_json(tx_hash: alloy::primitives::B256) -> serde_json::Value {
        json!({
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
    async fn test_fetch_after_clear_multiple_logs_picks_first_match() {
        let env = create_test_env();
        let cache = SymbolCache::default();

        let order = get_test_order_v4();
        let different_order = {
            let mut order = get_test_order_v4();
            order.nonce =
                fixed_bytes!("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
            order
        };

        let clear_event = create_clear_event(order.clone(), different_order);
        let orderbook = address!("0x1111111111111111111111111111111111111111");
        let tx_hash =
            fixed_bytes!("0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");

        let clear_log = create_test_log(orderbook, tx_hash, clear_event.to_log_data(), 5);

        let after_clear_event_1 =
            create_parameterized_after_clear_event(0xaa, "9000000000000000000", 100_000_000);
        let after_clear_event_2 =
            create_parameterized_after_clear_event(0xbb, "5000000000000000000", 50_000_000);

        let after_clear_log_1 =
            create_test_log(orderbook, tx_hash, after_clear_event_1.to_log_data(), 6);
        let after_clear_log_2 =
            create_test_log(orderbook, tx_hash, after_clear_event_2.to_log_data(), 7);

        let asserter = Asserter::new();
        asserter.push_success(&json!([after_clear_log_1, after_clear_log_2]));
        let receipt_json = create_test_receipt_json(tx_hash);
        asserter.push_success(&receipt_json);
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"USDC".to_string(),
        ));
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"AAPL0x".to_string(),
        ));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let feed_id_cache = FeedIdCache::default();

        let result = OnchainTrade::try_from_clear_v3(
            &env,
            &cache,
            provider,
            clear_event,
            clear_log,
            &feed_id_cache,
        )
        .await
        .unwrap();

        let trade = result.unwrap();
        assert_eq!(trade.symbol, tokenized_symbol!("AAPL0x"));
        assert!((trade.amount - 9.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_fetch_after_clear_equal_log_index_rejected() {
        let env = create_test_env();
        let cache = SymbolCache::default();

        let order = get_test_order_v4();
        let different_order = {
            let mut order = get_test_order_v4();
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
        asserter.push_success(&json!([after_clear_log_equal_index]));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let feed_id_cache = FeedIdCache::default();

        let result = OnchainTrade::try_from_clear_v3(
            &env,
            &cache,
            provider,
            clear_event,
            clear_log,
            &feed_id_cache,
        )
        .await;

        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(crate::error::TradeValidationError::NoAfterClearLog)
        ));
    }

    #[tokio::test]
    async fn test_fetch_after_clear_mixed_transactions_finds_correct() {
        let env = create_test_env();
        let cache = SymbolCache::default();

        let order = get_test_order_v4();
        let different_order = {
            let mut order = get_test_order_v4();
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
        let after_clear_event_wrong =
            create_parameterized_after_clear_event(0xcc, "1000000000000000000", 10_000_000);

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
        asserter.push_success(&json!([wrong_tx_log, correct_log]));
        let receipt_json = create_test_receipt_json(target_tx_hash);
        asserter.push_success(&receipt_json);
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"USDC".to_string(),
        ));
        asserter.push_success(&<symbolCall as SolCall>::abi_encode_returns(
            &"AAPL0x".to_string(),
        ));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let feed_id_cache = FeedIdCache::default();

        let result = OnchainTrade::try_from_clear_v3(
            &env,
            &cache,
            provider,
            clear_event,
            clear_log,
            &feed_id_cache,
        )
        .await
        .unwrap();

        let trade = result.unwrap();
        assert_eq!(trade.symbol, tokenized_symbol!("AAPL0x"));
        assert!((trade.amount - 9.0).abs() < f64::EPSILON);
    }
}

use alloy::primitives::{Address, B256};
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

#[derive(Debug, Clone, PartialEq)]
pub struct OnchainTrade {
    pub(crate) id: Option<i64>,
    pub(crate) tx_hash: B256,
    pub(crate) log_index: u64,
    pub(crate) symbol: TokenizedEquitySymbol,
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
    pub async fn save_within_transaction(
        &self,
        sql_tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    ) -> Result<i64, OnChainError> {
        let tx_hash_str = self.tx_hash.to_string();
        let log_index_i64 = i64::try_from(self.log_index)?;

        let direction_str = self.direction.as_str();
        let symbol_str = self.symbol.to_string();
        let block_timestamp_naive = self.block_timestamp.map(|dt| dt.naive_utc());

        let gas_used_i64 = self.gas_used.and_then(|g| i64::try_from(g).ok());
        let effective_gas_price_i64 = self.effective_gas_price.and_then(|p| i64::try_from(p).ok());

        let result = sqlx::query!(
            r#"
            INSERT INTO onchain_trades (
                tx_hash,
                log_index,
                symbol,
                amount,
                direction,
                price_usdc,
                block_timestamp,
                gas_used,
                effective_gas_price,
                pyth_price,
                pyth_confidence,
                pyth_exponent,
                pyth_publish_time
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
            "#,
            tx_hash_str,
            log_index_i64,
            symbol_str,
            self.amount,
            direction_str,
            self.price,
            block_timestamp_naive,
            gas_used_i64,
            effective_gas_price_i64,
            self.pyth_price,
            self.pyth_confidence,
            self.pyth_exponent,
            self.pyth_publish_time
        )
        .execute(&mut **sql_tx)
        .await?;

        Ok(result.last_insert_rowid())
    }

    #[cfg(test)]
    pub async fn find_by_tx_hash_and_log_index(
        pool: &sqlx::SqlitePool,
        tx_hash: B256,
        log_index: u64,
    ) -> Result<Self, OnChainError> {
        let tx_hash_str = tx_hash.to_string();
        let log_index_i64 = i64::try_from(log_index).expect("test log_index should fit in i64");

        let row = sqlx::query!(
            "
            SELECT
                id,
                tx_hash,
                log_index,
                symbol,
                amount,
                direction,
                price_usdc,
                created_at,
                block_timestamp,
                gas_used,
                effective_gas_price,
                pyth_price,
                pyth_confidence,
                pyth_exponent,
                pyth_publish_time
            FROM onchain_trades
            WHERE tx_hash = ?1 AND log_index = ?2
            ",
            tx_hash_str,
            log_index_i64
        )
        .fetch_one(pool)
        .await?;

        let tx_hash = row
            .tx_hash
            .parse()
            .expect("test db should have valid tx_hash");

        let direction = row.direction.parse()?;

        Ok(Self {
            id: Some(row.id),
            tx_hash,
            log_index: u64::try_from(row.log_index)
                .expect("test db log_index should be non-negative"),
            symbol: row.symbol.parse::<TokenizedEquitySymbol>().unwrap(),
            amount: row.amount,
            direction,
            price: Usdc::new(row.price_usdc).expect("db price_usdc should be non-negative"),
            block_timestamp: row
                .block_timestamp
                .map(|naive_dt| DateTime::from_naive_utc_and_offset(naive_dt, Utc)),
            created_at: row
                .created_at
                .map(|naive_dt| DateTime::from_naive_utc_and_offset(naive_dt, Utc)),
            gas_used: row.gas_used.and_then(|g| u64::try_from(g).ok()),
            effective_gas_price: row.effective_gas_price.and_then(|p| u128::try_from(p).ok()),
            pyth_price: row.pyth_price,
            pyth_confidence: row.pyth_confidence,
            pyth_exponent: row.pyth_exponent.and_then(|exp| i32::try_from(exp).ok()),
            pyth_publish_time: row
                .pyth_publish_time
                .map(|naive_dt| DateTime::from_naive_utc_and_offset(naive_dt, Utc)),
        })
    }

    #[cfg(test)]
    pub async fn db_count(pool: &sqlx::SqlitePool) -> Result<i64, sqlx::Error> {
        let row = sqlx::query!("SELECT COUNT(*) as count FROM onchain_trades")
            .fetch_one(pool)
            .await?;
        Ok(row.count)
    }

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
        let tokenized_symbol_str = if onchain_input_symbol == "USDC" {
            onchain_output_symbol
        } else {
            onchain_input_symbol
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
        env: &EvmEnv,
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
                    && log.address() == env.orderbook
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
                env,
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
    env: &EvmEnv,
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
            env,
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
    use alloy::primitives::{Address, U256, fixed_bytes, uint};
    use alloy::providers::{ProviderBuilder, mock::Asserter};
    use rain_math_float::Float;

    use super::*;
    use crate::onchain::EvmEnv;
    use crate::symbol::cache::SymbolCache;
    use crate::test_utils::setup_test_db;

    #[tokio::test]
    async fn test_onchain_trade_save_within_transaction_and_find() {
        let pool = setup_test_db().await;

        let trade = OnchainTrade {
            id: None,
            tx_hash: fixed_bytes!(
                "0xbeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
            ),
            log_index: 42,
            symbol: "AAPL0x".parse::<TokenizedEquitySymbol>().unwrap(),
            amount: 10.0,
            direction: Direction::Sell,
            price: Usdc::new(150.25).unwrap(),
            block_timestamp: DateTime::from_timestamp(1_672_531_200, 0), // Jan 1, 2023 00:00:00 UTC
            created_at: None,
            gas_used: Some(21000),
            effective_gas_price: Some(2_000_000_000), // 2 gwei
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };

        let mut sql_tx = pool.begin().await.unwrap();
        let id = trade.save_within_transaction(&mut sql_tx).await.unwrap();
        sql_tx.commit().await.unwrap();
        assert!(id > 0);

        let found =
            OnchainTrade::find_by_tx_hash_and_log_index(&pool, trade.tx_hash, trade.log_index)
                .await
                .unwrap();

        assert_eq!(found.tx_hash, trade.tx_hash);
        assert_eq!(found.log_index, trade.log_index);
        assert_eq!(found.symbol, trade.symbol);
        assert!((found.amount - trade.amount).abs() < f64::EPSILON);
        assert_eq!(found.direction, trade.direction);
        assert_eq!(found.price, trade.price);
        assert_eq!(found.block_timestamp, trade.block_timestamp);
        assert_eq!(found.gas_used, trade.gas_used);
        assert_eq!(found.effective_gas_price, trade.effective_gas_price);
        assert!(found.id.is_some());
        assert!(found.created_at.is_some());
    }

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

    #[tokio::test]
    async fn test_find_by_tx_hash_and_log_index_invalid_formats() {
        let pool = setup_test_db().await;

        // Attempt to insert invalid tx_hash format - should fail due to constraint
        let insert_result = sqlx::query!(
            "INSERT INTO onchain_trades (tx_hash, log_index, symbol, amount, direction, price_usdc)
             VALUES ('invalid_hash', 1, 'TEST', 1.0, 'BUY', 1.0)"
        )
        .execute(&pool)
        .await;

        // The insert should fail due to tx_hash constraint
        assert!(insert_result.is_err());
    }

    #[tokio::test]
    async fn test_find_by_tx_hash_and_log_index_invalid_direction() {
        let pool = setup_test_db().await;

        // Attempt to insert invalid direction data - should fail due to constraint
        let insert_result = sqlx::query!(
            "INSERT INTO onchain_trades (tx_hash, log_index, symbol, amount, direction, price_usdc)
             VALUES ('0x1234567890123456789012345678901234567890123456789012345678901234', 1, 'TEST', 1.0, 'INVALID', 1.0)"
        )
        .execute(&pool)
        .await;

        // The insert should fail due to direction constraint
        assert!(insert_result.is_err());
    }

    #[tokio::test]
    async fn test_save_within_transaction_constraint_violation() {
        let pool = setup_test_db().await;

        let trade = OnchainTrade {
            id: None,
            tx_hash: fixed_bytes!(
                "0x1111111111111111111111111111111111111111111111111111111111111111"
            ),
            log_index: 100,
            symbol: "AAPL0x".parse::<TokenizedEquitySymbol>().unwrap(),
            amount: 10.0,
            direction: Direction::Buy,
            price: Usdc::new(150.0).unwrap(),
            block_timestamp: DateTime::from_timestamp(1_672_531_800, 0), // Jan 1, 2023 00:10:00 UTC
            created_at: None,
            gas_used: Some(50000), // Complex contract interaction
            effective_gas_price: Some(1_500_000_000), // 1.5 gwei in wei
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };

        // Insert first trade
        let mut sql_tx1 = pool.begin().await.unwrap();
        trade.save_within_transaction(&mut sql_tx1).await.unwrap();
        sql_tx1.commit().await.unwrap();

        // Try to insert duplicate trade (same tx_hash and log_index)
        let mut sql_tx2 = pool.begin().await.unwrap();
        let duplicate_result = trade.save_within_transaction(&mut sql_tx2).await;
        assert!(
            duplicate_result.is_err(),
            "Expected duplicate constraint violation"
        );
        sql_tx2.rollback().await.unwrap();
    }

    #[tokio::test]
    async fn test_save_within_transaction_large_log_index_wrapping() {
        let pool = setup_test_db().await;

        // Test that extremely large log_index values are handled consistently
        // u64::MAX will wrap to -1 when cast to i64
        let trade = OnchainTrade {
            id: None,
            tx_hash: fixed_bytes!(
                "0x2222222222222222222222222222222222222222222222222222222222222222"
            ),
            log_index: u64::MAX, // Will become -1 when cast to i64
            symbol: "AAPL0x".parse::<TokenizedEquitySymbol>().unwrap(),
            amount: 10.0,
            direction: Direction::Buy,
            price: Usdc::new(150.0).unwrap(),
            block_timestamp: None,
            created_at: None,
            gas_used: None,
            effective_gas_price: None,
            pyth_price: None,
            pyth_confidence: None,
            pyth_exponent: None,
            pyth_publish_time: None,
        };

        let mut sql_tx = pool.begin().await.unwrap();

        // This should fail due to log_index constraint (log_index >= 0)
        let save_result = trade.save_within_transaction(&mut sql_tx).await;
        assert!(save_result.is_err());
        sql_tx.rollback().await.unwrap();
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
        let env = EvmEnv {
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
            &env,
            &feed_id_cache,
            Address::ZERO,
        )
        .await;

        assert!(matches!(
            result.unwrap_err(),
            OnChainError::Validation(TradeValidationError::TransactionNotFound(_))
        ));
    }

    #[tokio::test]
    async fn test_find_by_tx_hash_database_error() {
        let pool = setup_test_db().await;

        // Close the pool to simulate database connection error
        pool.close().await;

        // Expected database connection error
        OnchainTrade::find_by_tx_hash_and_log_index(
            &pool,
            fixed_bytes!("0x5555555555555555555555555555555555555555555555555555555555555555"),
            1,
        )
        .await
        .unwrap_err();
    }

    #[tokio::test]
    async fn test_db_count_with_data() {
        let pool = setup_test_db().await;

        // Insert test data
        for i in 0..5 {
            let mut tx_hash_bytes = [0u8; 32];
            tx_hash_bytes[0..31].copy_from_slice(&[
                0x12, 0x34, 0x56, 0x78, 0x90, 0x12, 0x34, 0x56, 0x78, 0x90, 0x12, 0x34, 0x56, 0x78,
                0x90, 0x12, 0x34, 0x56, 0x78, 0x90, 0x12, 0x34, 0x56, 0x78, 0x90, 0x12, 0x34, 0x56,
                0x78, 0x90, 0x12,
            ]);
            tx_hash_bytes[31] = i;

            let trade = OnchainTrade {
                id: None,
                tx_hash: alloy::primitives::B256::from(tx_hash_bytes),
                log_index: u64::from(i),
                symbol: crate::onchain::io::TokenizedEquitySymbol::parse(&format!("TEST{i}0x"))
                    .unwrap(),
                amount: 10.0,
                direction: Direction::Buy,
                price: Usdc::new(150.0).unwrap(),
                block_timestamp: None,
                created_at: None,
                gas_used: None,
                effective_gas_price: None,
                pyth_price: None,
                pyth_confidence: None,
                pyth_exponent: None,
                pyth_publish_time: None,
            };

            let mut sql_tx = pool.begin().await.unwrap();
            trade.save_within_transaction(&mut sql_tx).await.unwrap();
            sql_tx.commit().await.unwrap();
        }

        let count = OnchainTrade::db_count(&pool).await.unwrap();
        assert_eq!(count, 5);
    }
}

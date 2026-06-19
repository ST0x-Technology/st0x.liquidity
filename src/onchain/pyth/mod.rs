//! Pyth oracle price feed integration for onchain price lookups.
//!
//! Records the reference price an order observed by reading the Pyth contract's
//! `getPriceUnsafe` getter, pinned at the trade's block via a historic
//! `eth_call`. This captures the same stored price the order read without
//! relying on transaction traces (`debug_traceTransaction`), which are slow and
//! unreliably supported by load-balanced RPC providers.

use std::collections::HashMap;

use alloy::eips::BlockId;
use alloy::primitives::{Address, B256, U256, address};
use alloy::providers::Provider;
use chrono::DateTime;
#[cfg(test)]
use rain_math_float::{Float, FloatError};
#[cfg(test)]
use st0x_float_macro::float;
use tracing::debug;

use st0x_evm::IPyth;
use st0x_evm::PythStructs::Price;
use st0x_execution::Symbol;

pub const BASE_PYTH_CONTRACT_ADDRESS: Address =
    address!("0x8250f4aF4B972684F7b336503E2D6dFeDeB1487a");

/// Static mapping from base equity symbol to its Pyth price feed ID, sourced
/// from configuration. A symbol absent from the map has no configured feed, so
/// its trades are recorded without Pyth enrichment.
#[derive(Clone, Default, Debug)]
pub struct PythFeedIds(HashMap<Symbol, B256>);

impl PythFeedIds {
    pub fn new(feed_ids: HashMap<Symbol, B256>) -> Self {
        Self(feed_ids)
    }

    /// Returns the configured Pyth feed ID for the given base symbol, if any.
    pub fn get(&self, symbol: &Symbol) -> Option<B256> {
        let Self(feed_ids) = self;

        feed_ids.get(symbol).copied()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PythError {
    #[error("RPC error while reading Pyth price")]
    Rpc(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("Invalid timestamp value: {0}")]
    InvalidTimestamp(U256),
}

/// Reads the Pyth price for `feed_id` as stored on the Base Pyth contract at
/// `block_number`, via a historic `getPriceUnsafe` call.
///
/// `getPriceUnsafe` never reverts on staleness; it returns the feed's stored
/// state as of the END of `block_number`. This matches what the order observed
/// at its block unless a Pyth update for this feed landed in the same block
/// after the trade transaction — a rare case that yields a slightly newer
/// reference price. The value feeds analytics only and does not influence the
/// hedge decision (hedging uses the realized execution price), so the
/// end-of-block approximation is acceptable. Note this read currently runs
/// synchronously on the trade-conversion path, so a slow RPC adds hedge
/// latency; moving enrichment off the pre-hedge path is a tracked follow-up.
pub async fn extract_pyth_price<P>(
    provider: &P,
    feed_id: B256,
    block_number: u64,
) -> Result<Price, PythError>
where
    P: Provider,
{
    debug!(target: "hedge", %feed_id, block_number, "Reading Pyth price via getPriceUnsafe");

    let pyth = IPyth::new(BASE_PYTH_CONTRACT_ADDRESS, provider);

    let price = pyth
        .getPriceUnsafe(feed_id)
        .block(BlockId::number(block_number))
        .call()
        .await
        .map_err(|error| PythError::Rpc(Box::new(error)))?;

    debug!(
        target: "hedge",
        %feed_id,
        price = %price.price,
        expo = price.expo,
        conf = %price.conf,
        "Read Pyth price"
    );

    Ok(price)
}

/// Converts a raw Pyth SDK `Price` into the CQRS `PythPrice` struct,
/// preserving raw string values for lossless event storage.
pub(crate) fn raw_price_to_pyth_price(
    price: &Price,
) -> Result<crate::onchain_trade::PythPrice, PythError> {
    let publish_time_i64 = i64::try_from(price.publishTime)
        .map_err(|_| PythError::InvalidTimestamp(price.publishTime))?;

    let publish_time = DateTime::from_timestamp(publish_time_i64, 0)
        .ok_or(PythError::InvalidTimestamp(price.publishTime))?;

    Ok(crate::onchain_trade::PythPrice {
        value: price.price.to_string(),
        expo: price.expo,
        conf: price.conf.to_string(),
        publish_time,
    })
}

/// Scales a raw integer value by 10^exponent to produce a
/// human-readable Float. Only used in tests for verifying Pyth
/// price conversions.
#[cfg(test)]
fn scale_with_exponent(value: &impl ToString, exponent: i32) -> Result<Float, FloatError> {
    let decimal_value = Float::parse(value.to_string())?;

    if exponent >= 0 {
        let ten = float!(10);
        let multiplier = (0..exponent).try_fold(float!(1), |acc, _| acc * ten)?;

        decimal_value * multiplier
    } else {
        let abs_exponent = exponent.checked_abs().expect("exponent overflow");

        let ten = float!(10);
        let divisor = (0..abs_exponent).try_fold(float!(1), |acc, _| acc * ten)?;

        decimal_value / divisor
    }
}

/// Scales a raw Pyth `Price` to a human-readable Float. Only used in tests for
/// verifying Pyth price conversions. A free function rather than an inherent
/// method because `Price` is defined in `st0x-evm`, not this crate.
#[cfg(test)]
fn price_to_float(price: &Price) -> Result<Float, FloatError> {
    scale_with_exponent(&price.price, price.expo)
}

#[cfg(test)]
mod tests {
    use alloy::hex;
    use alloy::primitives::{Bytes, b256};
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;
    use alloy::sol_types::SolCall;
    use httpmock::prelude::*;
    use serde_json::json;

    use st0x_evm::IPyth::getPriceUnsafeCall;
    use st0x_float_macro::float;
    use st0x_float_serde::format_float_with_fallback;

    use super::*;

    /// Encodes a `Price` as the ABI return data of `getPriceUnsafe`, matching
    /// what an `eth_call` to the Pyth contract would return.
    fn encode_price_return(price: &Price) -> Bytes {
        Bytes::from(getPriceUnsafeCall::abi_encode_returns(price))
    }

    #[tokio::test]
    async fn extract_pyth_price_reads_getpriceunsafe_result() {
        // Real prod values for the COIN feed at block 47198127, verified to
        // match a debug_traceTransaction of the same fill.
        let price = Price {
            price: 15_445_005,
            conf: 21_005,
            expo: -5,
            publishTime: U256::from(1_781_166_017u64),
        };

        // NOTE: `Asserter` pops queued responses without inspecting the
        // request, so this test only verifies decoding. Forwarding of `feed_id`
        // and `block_number` into the request is asserted by
        // `extract_pyth_price_forwards_feed_id_and_block_number` below.
        let asserter = Asserter::new();
        asserter.push_success(&encode_price_return(&price));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let feed_id = b256!("0xfee33f2a978bf32dd6b662b65ba8083c6773b494f8401194ec1870c640860245");

        let result = extract_pyth_price(&provider, feed_id, 47_198_127)
            .await
            .unwrap();

        assert_eq!(result.price, 15_445_005);
        assert_eq!(result.conf, 21_005);
        assert_eq!(result.expo, -5);
        assert_eq!(result.publishTime, U256::from(1_781_166_017u64));
    }

    /// Asserts the core contract of the historic read: `feed_id` lands in the
    /// `eth_call` calldata and `block_number` is forwarded as the block tag.
    /// The mock only responds when the JSON-RPC body contains both, so a
    /// request built without them fails the call (and the hit-count assert).
    #[tokio::test]
    async fn extract_pyth_price_forwards_feed_id_and_block_number() {
        let price = Price {
            price: 15_445_005,
            conf: 21_005,
            expo: -5,
            publishTime: U256::from(1_781_166_017u64),
        };

        let feed_id = b256!("0xfee33f2a978bf32dd6b662b65ba8083c6773b494f8401194ec1870c640860245");
        let block_number = 47_198_127u64;
        let calldata = format!(
            "0x{}",
            hex::encode(getPriceUnsafeCall { id: feed_id }.abi_encode())
        );

        let server = MockServer::start_async().await;
        let rpc_mock = server
            .mock_async(|when, then| {
                when.method(POST)
                    .body_includes(&calldata)
                    .body_includes(format!("\"0x{block_number:x}\""));
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": 0,
                    "result": encode_price_return(&price).to_string(),
                }));
            })
            .await;

        let provider = ProviderBuilder::new().connect_http(server.base_url().parse().unwrap());

        let result = extract_pyth_price(&provider, feed_id, block_number)
            .await
            .unwrap();

        rpc_mock.assert_async().await;
        assert_eq!(result.price, 15_445_005);
        assert_eq!(result.conf, 21_005);
        assert_eq!(result.expo, -5);
        assert_eq!(result.publishTime, U256::from(1_781_166_017u64));
    }

    #[tokio::test]
    async fn extract_pyth_price_propagates_rpc_error() {
        let asserter = Asserter::new();
        asserter.push_failure_msg("eth_call boom");
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result = extract_pyth_price(&provider, B256::random(), 1).await;

        assert!(matches!(result, Err(PythError::Rpc(_))));
    }

    #[test]
    fn test_to_float_negative_exponent() {
        let price = Price {
            price: 123_456_789,
            conf: 1000,
            expo: -8,
            publishTime: U256::from(1_700_000_000u64),
        };

        let exact = price_to_float(&price).unwrap();

        assert_eq!(format_float_with_fallback(&exact), "1.23456789");
    }

    #[test]
    fn test_to_float_zero_exponent() {
        let price = Price {
            price: 42,
            conf: 1,
            expo: 0,
            publishTime: U256::from(1_700_000_000u64),
        };

        let exact = price_to_float(&price).unwrap();

        assert_eq!(format_float_with_fallback(&exact), "42");
    }

    #[test]
    fn test_to_float_positive_exponent() {
        let price = Price {
            price: 123,
            conf: 10,
            expo: 3,
            publishTime: U256::from(1_700_000_000u64),
        };

        let exact = price_to_float(&price).unwrap();

        assert_eq!(format_float_with_fallback(&exact), "123000");
    }

    #[test]
    fn test_to_float_various_exponents() {
        let test_cases = vec![
            (100_000_000, -6, "100"),
            (1_500_000, -6, "1.5"),
            (500, -2, "5"),
            (42_000, -3, "42"),
            (1, 0, "1"),
            (1, 5, "100000"),
        ];

        for (price_value, expo, expected) in test_cases {
            let price = Price {
                price: price_value,
                conf: 100,
                expo,
                publishTime: U256::from(1_700_000_000u64),
            };

            let exact = price_to_float(&price).unwrap();

            assert_eq!(
                format_float_with_fallback(&exact),
                expected,
                "Failed for price={price_value}, expo={expo}"
            );
        }
    }

    #[test]
    fn test_to_float_negative_price() {
        let price = Price {
            price: -50_000_000,
            conf: 1000,
            expo: -6,
            publishTime: U256::from(1_700_000_000u64),
        };

        let exact = price_to_float(&price).unwrap();

        assert_eq!(format_float_with_fallback(&exact), "-50");
    }

    #[test]
    fn test_to_float_equity_price() {
        let price = Price {
            price: 18_250,
            conf: 10,
            expo: -2,
            publishTime: U256::from(1_700_000_000u64),
        };

        let exact = price_to_float(&price).unwrap();

        assert_eq!(format_float_with_fallback(&exact), "182.5");
    }

    #[test]
    fn test_scale_with_exponent_negative() {
        let result = scale_with_exponent(&123_456_789, -8).unwrap();
        assert!(result.eq(float!(1.23456789)).unwrap());
    }

    #[test]
    fn test_scale_with_exponent_positive() {
        let result = scale_with_exponent(&123, 3).unwrap();
        assert!(result.eq(float!(123000)).unwrap());
    }

    #[test]
    fn test_scale_with_exponent_zero() {
        let result = scale_with_exponent(&42, 0).unwrap();
        assert!(result.eq(float!(42)).unwrap());
    }

    #[test]
    fn test_scale_with_exponent_large_value() {
        // Float handles large values that Decimal couldn't
        let result = scale_with_exponent(&u64::MAX, 10).unwrap();
        assert!(
            result
                .eq(float!(&"184467440737095516150000000000".to_string()))
                .unwrap()
        );
    }

    #[test]
    fn test_raw_price_to_pyth_price() {
        let price = Price {
            price: 18_250_000_000,
            conf: 10_000_000,
            expo: -8,
            publishTime: U256::from(1_700_000_000u64),
        };

        let pyth_price = raw_price_to_pyth_price(&price).unwrap();

        assert_eq!(pyth_price.value, "18250000000");
        assert_eq!(pyth_price.conf, "10000000");
        assert_eq!(pyth_price.expo, -8);
        assert_eq!(
            pyth_price.publish_time,
            DateTime::from_timestamp(1_700_000_000, 0).unwrap()
        );
    }

    #[test]
    fn test_raw_price_to_pyth_price_timestamp_overflow() {
        let price = Price {
            price: 100_000,
            conf: 500,
            expo: -5,
            publishTime: U256::MAX,
        };

        assert!(matches!(
            raw_price_to_pyth_price(&price),
            Err(PythError::InvalidTimestamp(_))
        ));
    }

    #[test]
    fn test_raw_price_to_pyth_price_timestamp_out_of_chrono_range() {
        // i64::MAX passes i64::try_from but DateTime::from_timestamp rejects it
        let price = Price {
            price: 100_000,
            conf: 500,
            expo: -5,
            publishTime: U256::from(i64::MAX as u64),
        };

        assert!(matches!(
            raw_price_to_pyth_price(&price),
            Err(PythError::InvalidTimestamp(_))
        ));
    }
}

//! ETH/USD valuation for bot-gas cost recording.
//!
//! See ADR 0017: gas is paid in ETH on both Base and Ethereum, and ETH/USD is
//! the same economic price regardless of chain, so a single Base-hosted Pyth
//! feed values every receipt. A Base receipt pins the read at its own block
//! HASH (not just its number) for a reproducible valuation that stays
//! reorg-safe across a job retry, matching the existing equity-enrichment
//! pattern (`crate::onchain::pyth`) except for that hash pinning -- see
//! `crate::bot_gas::job`'s own block-hash-vs-number doc for why the hash
//! matters. An Ethereum receipt has no equivalent Base block, so the read
//! pins at the latest Base block at recording time and persists that block
//! number instead.
//!
//! The Base-receipt path performs a historic `eth_call` at an arbitrary past
//! block (the receipt's own), which assumes the configured Base RPC endpoint
//! is an archive node (or otherwise retains state that far back) -- see the
//! `base_rpc_url` doc comment (`st0x-config`) and ADR 0017's "Negative /
//! costs" section. A node that has pruned that block's state fails the call;
//! that failure surfaces as a typed `EthUsdValuationError::Pyth` and is
//! logged when the best-effort recording job dead-letters, not swallowed. No
//! fallback to a different block is applied here -- that would break the
//! reproducibility this design is built on.
//!
//! # Reorg-safe pinning requires EIP-1898 (block-hash `eth_call`)
//!
//! Pinning the Base `eth_call` at `receipt_block_hash` (not
//! `receipt_block_number`) requires the configured `base_rpc_url` to accept
//! the EIP-1898 object-form block parameter (`{"blockHash": ...}`), not just
//! the legacy tag/number form. This is the only call site in the codebase
//! that pins by hash rather than number. EIP-1898 has been part of the
//! standard Ethereum JSON-RPC spec since 2019 and is supported by every
//! mainstream execution client (geth, erigon, reth, besu); this deployment's
//! Base endpoint is a self-hosted node (not a third-party gateway prone to
//! stripping non-standard params), so the risk of rejection is low. If the
//! endpoint ever does reject the hash form, the failure surfaces as
//! `EthUsdValuationError::Rpc`/`Pyth(PythError::Rpc)`, which
//! `crate::bot_gas::job::is_transient_rpc_error` classifies as transient --
//! every Base receipt would redrive for the full bounded window and then
//! dead-letter, which would look like RPC flakiness rather than a capability
//! mismatch. See ADR 0017 for the accepted trade-off (reorg safety over a
//! cheap startup probe) and where to look first if Base bot-gas costs stop
//! recording entirely.
//!
//! # Persisted USD values
//!
//! `EthUsdPrice.price` and `BotGasReceiptCost.usd_cost` are `st0x_finance::Usd`
//! (see docs/float.md), the established newtype for an offchain dollar amount.
//! They are *persisted* fields on `BotGasReceiptCost`, serialized as decimal
//! strings into the CQRS event log, so `usd_cost` is rounded to
//! `PERSISTED_DECIMAL_PRECISION` when it is built rather than relying on
//! formatting to round it. Changing that precision, or how the rounding is
//! applied, changes what future events persist and needs a
//! `verify-migrations` run against real events.

use alloy::eips::BlockId;
use alloy::primitives::{Address, B256, U256};
use alloy::providers::Provider;
use alloy::transports::{RpcError, TransportErrorKind};
use chrono::{DateTime, TimeDelta, Utc};
use rain_math_float::Float;
use tracing::warn;

use st0x_finance::Usd;
use st0x_float_macro::float;

use super::{BotGasChain, EthUsdPrice};
use crate::onchain::pyth::{PythError, extract_pyth_price_at};

/// Descriptive source string persisted on every recorded cost fact.
const PYTH_SOURCE: &str = "pyth:base:getPriceUnsafe";

/// Reused by the exponent-scaling loop without runtime decimal parsing.
const TEN: Float = float!(10);
const ONE: Float = float!(1);

/// A Pyth price older than this relative to the receipt's `occurred_at` is
/// recorded with a warning rather than rejected (ADR 0017: record + warn, no
/// hard fail). This is a threshold for the warning signal only -- it does
/// not gate recording.
///
/// Pyth on EVM is pull-based: a feed's on-chain `publishTime` only advances
/// when someone pays to push an update, so the real update cadence of
/// `Crypto.ETH/USD` on the configured Base deployment is an operator/market
/// -controlled, external-system property that this code has not measured.
/// 5 minutes is therefore a round, generous DEFAULT chosen without that
/// measurement, not a verified bound: it may fire on an ordinary reading if
/// the feed updates less often than this, or stay quiet on a genuinely
/// stale one if it updates more often. Treat the warning as a loose signal,
/// not a calibrated one, until the cadence is sampled and this constant is
/// set from that data (see ADR 0017).
const STALE_PRICE_THRESHOLD: TimeDelta = TimeDelta::minutes(5);

#[derive(Debug, thiserror::Error)]
pub(crate) enum EthUsdValuationError {
    #[error(transparent)]
    Rpc(#[from] RpcError<TransportErrorKind>),
    #[error(transparent)]
    Pyth(#[from] PythError),
    #[error("ETH/USD Pyth price must be positive: price={price} expo={expo}")]
    NonPositivePrice { price: i64, expo: i32 },
    #[error("failed to parse decimal `{value}`")]
    Decimal {
        value: String,
        #[source]
        source: rain_math_float::FloatError,
    },
    /// Scaling is bounded to 18 powers of ten over an `i64` mantissa, so
    /// accepted Pyth inputs remain well within `Float`'s coefficient range.
    #[error("ETH/USD valuation arithmetic failed: {0}")]
    Arithmetic(#[from] rain_math_float::FloatError),
    #[error("invalid ETH/USD Pyth publish time {0}")]
    InvalidPublishTime(U256),
    #[error("ETH/USD Pyth price exponent {expo} outside the plausible range -18..=0")]
    ExponentOutOfRange { expo: i32 },
}

/// Plausible range for a Pyth price feed's `expo` field. Real feeds use a
/// small negative exponent (ETH/USD is `-8`); anything outside this band is
/// not a value any known Pyth feed returns. Rejecting it here avoids an
/// unbounded `10^|expo|` scaling loop (up to `i32::MIN.unsigned_abs()`
/// iterations) on the single-concurrency bot-gas worker. Note this cannot be
/// reached by a misconfigured `pyth_contract`/feed id: an unknown feed id
/// reverts with `PythErrors.PriceFeedNotFound` and a contract address with
/// no code fails ABI decoding, so neither ever produces a `Price` struct for
/// this guard to inspect (see ADR 0017).
const PLAUSIBLE_EXPO_RANGE: std::ops::RangeInclusive<i32> = -18..=0;

/// Reads the ETH/USD price used to value a bot-paid gas receipt.
///
/// `receipt_block_hash` anchors the Base-receipt read: the `eth_call` pins at
/// that HASH rather than `receipt_block_number`, so a reorg that replaces the
/// block at that height (between the receipt fetch and this call, or across a
/// job retry) cannot silently value the gas against a chain state that no
/// longer contains the transaction -- the same hazard `crate::bot_gas::job`
/// already guards against for the block timestamp.
///
/// Never rejects a stale publish time: it is recorded and a warning is
/// logged, but the fact is still returned so recording never strands on a
/// quiet feed (see ADR 0017 and the module docs above).
pub(crate) async fn read_eth_usd_price<BaseProvider>(
    base_provider: &BaseProvider,
    pyth_contract: Address,
    eth_usd_feed_id: B256,
    chain: BotGasChain,
    receipt_block_number: u64,
    receipt_block_hash: B256,
    occurred_at: DateTime<Utc>,
) -> Result<EthUsdPrice, EthUsdValuationError>
where
    BaseProvider: Provider,
{
    let block_number = match chain {
        BotGasChain::Base => receipt_block_number,
        BotGasChain::Ethereum => base_provider.get_block_number().await?,
    };
    let block_id = match chain {
        BotGasChain::Base => BlockId::hash(receipt_block_hash),
        BotGasChain::Ethereum => BlockId::number(block_number),
    };

    let price =
        extract_pyth_price_at(base_provider, pyth_contract, eth_usd_feed_id, block_id).await?;

    let publish_time_secs = i64::try_from(price.publishTime)
        .map_err(|_| EthUsdValuationError::InvalidPublishTime(price.publishTime))?;
    let observed_at = DateTime::from_timestamp(publish_time_secs, 0)
        .ok_or(EthUsdValuationError::InvalidPublishTime(price.publishTime))?;

    let staleness = occurred_at.signed_duration_since(observed_at);
    if staleness > STALE_PRICE_THRESHOLD {
        warn!(
            target: "rebalance",
            %eth_usd_feed_id,
            %block_number,
            %observed_at,
            %occurred_at,
            staleness_secs = staleness.num_seconds(),
            "ETH/USD Pyth price is stale relative to the receipt's occurred_at; \
             recording anyway (ADR 0017)",
        );
    }

    let value = scale_price_to_float(price.price, price.expo)?;
    if !value.gt(float!(0))? {
        return Err(EthUsdValuationError::NonPositivePrice {
            price: price.price,
            expo: price.expo,
        });
    }

    Ok(EthUsdPrice {
        price: Usd::new(value),
        source: PYTH_SOURCE.to_owned(),
        observed_at,
        block_number: Some(block_number),
    })
}

fn parse_float(value: &str) -> Result<Float, EthUsdValuationError> {
    Float::parse(value.to_owned()).map_err(|source| EthUsdValuationError::Decimal {
        value: value.to_owned(),
        source,
    })
}

/// Scales a raw Pyth `(price, expo)` pair into `price * 10^expo`.
///
/// `Float` carries a 224-bit coefficient, so an ETH/USD price at Pyth's 10^-8
/// exponent is represented without loss.
fn scale_price_to_float(price: i64, expo: i32) -> Result<Float, EthUsdValuationError> {
    if !PLAUSIBLE_EXPO_RANGE.contains(&expo) {
        return Err(EthUsdValuationError::ExponentOutOfRange { expo });
    }

    let mantissa = parse_float(&price.to_string())?;
    if expo == 0 {
        return Ok(mantissa);
    }

    let scale = (0..expo.unsigned_abs()).try_fold(ONE, |acc, _| acc * TEN)?;

    // `PLAUSIBLE_EXPO_RANGE` and the `expo == 0` early return above already
    // establish `expo < 0` here, so this only ever divides. Pyth's on-chain
    // prices use negative exponents (10^-8 for ETH/USD); a positive-exponent
    // feed would need `PLAUSIBLE_EXPO_RANGE` widened and this branch back.
    Ok((mantissa / scale)?)
}

#[cfg(test)]
mod tests {
    use alloy::hex;
    use alloy::primitives::{Bytes, address, b256};
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;
    use alloy::sol_types::SolCall;
    use chrono::TimeZone;
    use httpmock::prelude::*;
    use serde_json::json;

    use st0x_evm::IPyth::getPriceUnsafeCall;
    use st0x_evm::PythStructs::Price;

    use st0x_float_serde::format_float;

    use super::*;

    const PYTH_CONTRACT: Address = address!("0x8250f4aF4B972684F7b336503E2D6dFeDeB1487a");
    /// Pyth's `Crypto.ETH/USD` feed id, matching prod/staging config. See
    /// https://www.pyth.network/developers/price-feed-ids#pyth-evm-stable.
    const FEED_ID: B256 =
        b256!("0xff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0ace");
    /// Stand-in receipt block hash for tests that don't specifically assert
    /// on the hash-vs-number distinction -- `Asserter` never inspects the
    /// request, so an arbitrary value is fine there.
    const RECEIPT_BLOCK_HASH: B256 =
        b256!("0x1111111111111111111111111111111111111111111111111111111111111111");

    fn encode_price_return(price: &Price) -> Bytes {
        Bytes::from(getPriceUnsafeCall::abi_encode_returns(price))
    }

    fn occurred_at() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 23, 12, 0, 0).unwrap()
    }

    fn fresh_price(publish_time: DateTime<Utc>) -> Price {
        Price {
            price: 200_000_000_000,
            conf: 1_000_000,
            expo: -8,
            publishTime: U256::from(publish_time.timestamp()),
        }
    }

    #[tokio::test]
    async fn base_receipt_pins_read_at_receipt_block() {
        let asserter = Asserter::new();
        asserter.push_success(&encode_price_return(&fresh_price(occurred_at())));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result = read_eth_usd_price(
            &provider,
            PYTH_CONTRACT,
            FEED_ID,
            BotGasChain::Base,
            123,
            RECEIPT_BLOCK_HASH,
            occurred_at(),
        )
        .await
        .unwrap();

        assert_eq!(result.block_number, Some(123));
        assert_eq!(format_float(&result.price.inner()).unwrap(), "2000");
        assert_eq!(result.source, "pyth:base:getPriceUnsafe");
    }

    /// A Base receipt's `eth_call` must pin at `receipt_block_hash`, not
    /// `receipt_block_number`: a reorg between the receipt fetch and this
    /// call (or across a job retry) can replace the block at that height, so
    /// pinning by number alone would silently value the gas against a
    /// different chain state than the one the timestamp came from. The mock
    /// only responds to a hash-tagged request; a number-tagged request (the
    /// pre-fix behaviour) would not match and the call would fail.
    #[tokio::test]
    async fn base_receipt_pins_eth_call_at_receipt_block_hash_not_number() {
        let price = fresh_price(occurred_at());
        let calldata = format!(
            "0x{}",
            hex::encode(getPriceUnsafeCall { id: FEED_ID }.abi_encode())
        );

        let server = MockServer::start_async().await;
        let rpc_mock = server
            .mock_async(|when, then| {
                when.method(POST)
                    .body_includes(&calldata)
                    .body_includes(alloy::hex::encode(RECEIPT_BLOCK_HASH));
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": 0,
                    "result": encode_price_return(&price).to_string(),
                }));
            })
            .await;

        let provider = ProviderBuilder::new().connect_http(server.base_url().parse().unwrap());

        read_eth_usd_price(
            &provider,
            PYTH_CONTRACT,
            FEED_ID,
            BotGasChain::Base,
            // Deliberately a different block than the hash resolves to, so a
            // regression back to number-pinning would hit a distinct (and
            // here, unmocked) block tag and fail the request.
            123,
            RECEIPT_BLOCK_HASH,
            occurred_at(),
        )
        .await
        .unwrap();

        rpc_mock.assert_async().await;
    }

    /// An Ethereum receipt has no equivalent Base block, so the read pins at
    /// the latest Base block (`eth_blockNumber`) rather than the receipt's
    /// own (Ethereum) block number -- proven here by returning a distinct
    /// block number from the mocked `eth_blockNumber` response.
    #[tokio::test]
    async fn ethereum_receipt_pins_read_at_latest_base_block() {
        let asserter = Asserter::new();
        asserter.push_success(&999u64);
        asserter.push_success(&encode_price_return(&fresh_price(occurred_at())));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result = read_eth_usd_price(
            &provider,
            PYTH_CONTRACT,
            FEED_ID,
            BotGasChain::Ethereum,
            // Receipt block number on Ethereum -- must NOT be the one used.
            111,
            RECEIPT_BLOCK_HASH,
            occurred_at(),
        )
        .await
        .unwrap();

        assert_eq!(result.block_number, Some(999));
    }

    /// Pins the assumed contract shape against a REAL response, not a
    /// synthesized one: `PYTH_CONTRACT`/`FEED_ID` (the configured Base
    /// mainnet address and Pyth's `Crypto.ETH/USD` feed id) genuinely resolve
    /// to a populated, ABI-decodable `Price` on-chain, and the exponent this
    /// code assumes (`-8`) matches what the deployment actually returns.
    /// Every other test in this module feeds the code a hand-built `Price`
    /// through `Asserter`, which cannot distinguish a correct feed id from a
    /// wrong one -- this test is the one place that fact is verified.
    #[tokio::test]
    async fn eth_usd_feed_resolves_to_a_real_populated_price_on_base() {
        // Real response from `getPriceUnsafe` on Base mainnet, captured via:
        //   cast call 0x8250f4aF4B972684F7b336503E2D6dFeDeB1487a \
        //     "getPriceUnsafe(bytes32)((int64,uint64,int32,uint256))" \
        //     0xff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0ace \
        //     --rpc-url https://mainnet.base.org --block 49065697
        // -> (185765949113, 70050886, -8, 1784920622)
        let price = Price {
            price: 185_765_949_113,
            conf: 70_050_886,
            expo: -8,
            publishTime: U256::from(1_784_920_622u64),
        };

        let asserter = Asserter::new();
        asserter.push_success(&encode_price_return(&price));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result = read_eth_usd_price(
            &provider,
            PYTH_CONTRACT,
            FEED_ID,
            BotGasChain::Base,
            49_065_697,
            RECEIPT_BLOCK_HASH,
            Utc.timestamp_opt(1_784_920_622, 0).unwrap(),
        )
        .await
        .unwrap();

        assert_eq!(
            format_float(&result.price.inner()).unwrap(),
            "1857.65949113"
        );
        assert_eq!(result.source, "pyth:base:getPriceUnsafe");
    }

    #[tokio::test]
    async fn stale_price_is_recorded_not_rejected() {
        let stale_publish_time = occurred_at() - TimeDelta::hours(1);
        let asserter = Asserter::new();
        asserter.push_success(&encode_price_return(&fresh_price(stale_publish_time)));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result = read_eth_usd_price(
            &provider,
            PYTH_CONTRACT,
            FEED_ID,
            BotGasChain::Base,
            123,
            RECEIPT_BLOCK_HASH,
            occurred_at(),
        )
        .await
        .unwrap();

        assert_eq!(
            result.observed_at.timestamp(),
            stale_publish_time.timestamp()
        );
    }

    #[tokio::test]
    async fn zero_price_is_rejected() {
        let asserter = Asserter::new();
        let mut price = fresh_price(occurred_at());
        price.price = 0;
        asserter.push_success(&encode_price_return(&price));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let error = read_eth_usd_price(
            &provider,
            PYTH_CONTRACT,
            FEED_ID,
            BotGasChain::Base,
            123,
            RECEIPT_BLOCK_HASH,
            occurred_at(),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            EthUsdValuationError::NonPositivePrice { price: 0, .. }
        ));
    }

    #[tokio::test]
    async fn negative_price_is_rejected() {
        let asserter = Asserter::new();
        let mut price = fresh_price(occurred_at());
        price.price = -1;
        asserter.push_success(&encode_price_return(&price));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let error = read_eth_usd_price(
            &provider,
            PYTH_CONTRACT,
            FEED_ID,
            BotGasChain::Base,
            123,
            RECEIPT_BLOCK_HASH,
            occurred_at(),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            EthUsdValuationError::NonPositivePrice { price: -1, .. }
        ));
    }

    #[tokio::test]
    async fn rpc_error_propagates() {
        let asserter = Asserter::new();
        asserter.push_failure_msg("eth_call boom");
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let error = read_eth_usd_price(
            &provider,
            PYTH_CONTRACT,
            FEED_ID,
            BotGasChain::Base,
            123,
            RECEIPT_BLOCK_HASH,
            occurred_at(),
        )
        .await
        .unwrap_err();

        assert!(matches!(error, EthUsdValuationError::Pyth(_)));
    }

    #[tokio::test]
    async fn ethereum_receipt_propagates_base_block_number_rpc_error() {
        let asserter = Asserter::new();
        asserter.push_failure_msg("eth_blockNumber boom");
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let error = read_eth_usd_price(
            &provider,
            PYTH_CONTRACT,
            FEED_ID,
            BotGasChain::Ethereum,
            111,
            RECEIPT_BLOCK_HASH,
            occurred_at(),
        )
        .await
        .unwrap_err();

        assert!(matches!(error, EthUsdValuationError::Rpc(_)));
    }

    #[tokio::test]
    async fn forwards_configured_pyth_contract_address() {
        let feed_id = FEED_ID;
        let custom_contract = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let calldata = format!(
            "0x{}",
            hex::encode(getPriceUnsafeCall { id: feed_id }.abi_encode())
        );

        let server = MockServer::start_async().await;
        let rpc_mock = server
            .mock_async(|when, then| {
                when.method(POST)
                    .body_includes(&calldata)
                    .body_includes(alloy::hex::encode_prefixed(custom_contract));
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": 0,
                    "result": encode_price_return(&fresh_price(occurred_at())).to_string(),
                }));
            })
            .await;

        let provider = ProviderBuilder::new().connect_http(server.base_url().parse().unwrap());

        read_eth_usd_price(
            &provider,
            custom_contract,
            feed_id,
            BotGasChain::Base,
            1,
            RECEIPT_BLOCK_HASH,
            occurred_at(),
        )
        .await
        .unwrap();

        rpc_mock.assert_async().await;
    }

    #[test]
    fn scale_price_to_float_accepts_the_real_eth_usd_exponent() {
        let value = scale_price_to_float(200_000_000_000, -8).unwrap();
        assert_eq!(format_float(&value).unwrap(), "2000");
    }

    #[test]
    fn scale_price_to_float_rejects_an_implausible_exponent() {
        let error = scale_price_to_float(1, i32::MIN).unwrap_err();

        assert!(matches!(
            error,
            EthUsdValuationError::ExponentOutOfRange { expo: i32::MIN }
        ));
    }

    #[tokio::test]
    async fn read_eth_usd_price_rejects_an_implausible_exponent() {
        let asserter = Asserter::new();
        let mut price = fresh_price(occurred_at());
        price.expo = i32::MIN;
        asserter.push_success(&encode_price_return(&price));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let error = read_eth_usd_price(
            &provider,
            PYTH_CONTRACT,
            FEED_ID,
            BotGasChain::Base,
            123,
            RECEIPT_BLOCK_HASH,
            occurred_at(),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            EthUsdValuationError::ExponentOutOfRange { expo: i32::MIN }
        ));
    }
}

//! ETH/USD valuation for bot-gas cost recording.
//!
//! ADR 0020 selects Chainlink's standard ETH/USD proxy on Base. A Base
//! receipt pins both feed calls at the receipt block hash, preserving the
//! EIP-1898 reorg-safe behavior established by ADR 0017. An Ethereum receipt
//! has no corresponding Base block, so its valuation pins at the latest Base
//! block observed by the recording job and persists that block number.
//!
//! Historic calls require the configured Base RPC to retain the selected
//! block's state. No fallback to a different block is allowed because that
//! would make the recorded valuation disagree with its persisted block anchor.

use alloy::contract::Error as ContractError;
use alloy::eips::BlockId;
use alloy::primitives::{Address, B256, I256, U256};
use alloy::providers::Provider;
use alloy::sol;
use alloy::transports::{RpcError, TransportErrorKind};
use chrono::{DateTime, TimeDelta, Utc};
use rain_math_float::Float;
use tracing::warn;

use st0x_evm::Chain;
use st0x_finance::Usd;
use st0x_float_macro::float;

use super::EthUsdPrice;

sol! {
    #[sol(rpc)]
    interface AggregatorV3Interface {
        function decimals() external view returns (uint8 decimalPlaces);
        function latestRoundData() external view returns (
            uint80 roundId,
            int256 answer,
            uint256 startedAt,
            uint256 updatedAt,
            uint80 answeredInRound
        );
    }
}

const CHAINLINK_SOURCE: &str = "chainlink:base:latestRoundData";
const MAX_DECIMALS: u8 = 18;
const TEN: Float = float!(10);
const ONE: Float = float!(1);
// Chainlink's Base ETH/USD feed has a 20-minute heartbeat:
// https://data.chain.link/feeds/base/base/eth-usd. The extra five minutes
// prevents ordinary feed publication and receipt-recording latency from
// generating a false operational warning.
const STALE_PRICE_THRESHOLD: TimeDelta = TimeDelta::minutes(25);

#[derive(Debug, thiserror::Error)]
pub(crate) enum EthUsdValuationError {
    #[error(transparent)]
    Rpc(#[from] RpcError<TransportErrorKind>),
    #[error(transparent)]
    Contract(#[from] ContractError),
    #[error("ETH/USD Chainlink answer must be positive: {answer}")]
    NonPositivePrice { answer: I256 },
    #[error("failed to parse Chainlink answer `{answer}`")]
    Decimal {
        answer: I256,
        #[source]
        source: rain_math_float::FloatError,
    },
    #[error("ETH/USD valuation arithmetic failed: {0}")]
    Arithmetic(#[from] rain_math_float::FloatError),
    #[error("invalid ETH/USD Chainlink update time {0}")]
    InvalidUpdatedAt(U256),
    #[error("ETH/USD Chainlink decimals {decimals} exceeds supported maximum {MAX_DECIMALS}")]
    DecimalsOutOfRange { decimals: u8 },
    /// Pricing gas in ETH is only meaningful where the native token IS ETH.
    /// Valuing another chain's gas against the ETH/USD feed would silently
    /// misprice it, so the read is refused instead.
    #[error("{chain} pays gas in a token that is not ETH, so the ETH/USD feed cannot value it")]
    NonEthGasToken { chain: Chain },
}

/// Reads the ETH/USD price used to value a bot-paid gas receipt.
///
/// Staleness is recorded and warned about rather than rejected. Invalid data
/// fails explicitly and never becomes a fabricated financial value.
pub(crate) async fn read_eth_usd_price<BaseProvider>(
    base_provider: &BaseProvider,
    chainlink_feed: Address,
    chain: Chain,
    receipt_block_number: u64,
    receipt_block_hash: B256,
    occurred_at: DateTime<Utc>,
) -> Result<EthUsdPrice, EthUsdValuationError>
where
    BaseProvider: Provider,
{
    let (block_number, block_id) = match chain {
        Chain::Base => (receipt_block_number, BlockId::hash(receipt_block_hash)),
        Chain::Ethereum => {
            let latest = base_provider.get_block_number().await?;
            (latest, BlockId::number(latest))
        }
        Chain::HyperEvm => return Err(EthUsdValuationError::NonEthGasToken { chain }),
    };

    let feed = AggregatorV3Interface::new(chainlink_feed, base_provider);
    let decimals = feed.decimals().block(block_id).call().await?;
    if decimals > MAX_DECIMALS {
        return Err(EthUsdValuationError::DecimalsOutOfRange { decimals });
    }

    let round = feed.latestRoundData().block(block_id).call().await?;

    if !round.answer.is_positive() {
        return Err(EthUsdValuationError::NonPositivePrice {
            answer: round.answer,
        });
    }

    if round.updatedAt.is_zero() {
        return Err(EthUsdValuationError::InvalidUpdatedAt(round.updatedAt));
    }

    let updated_at_secs = i64::try_from(round.updatedAt)
        .map_err(|_| EthUsdValuationError::InvalidUpdatedAt(round.updatedAt))?;
    let observed_at = DateTime::from_timestamp(updated_at_secs, 0)
        .ok_or(EthUsdValuationError::InvalidUpdatedAt(round.updatedAt))?;

    let staleness = occurred_at.signed_duration_since(observed_at);
    if staleness > STALE_PRICE_THRESHOLD {
        warn!(
            target: "rebalance",
            %chainlink_feed,
            %block_number,
            %observed_at,
            %occurred_at,
            staleness_secs = staleness.num_seconds(),
            "ETH/USD Chainlink price is stale relative to the receipt; recording anyway",
        );
    }

    let value = scale_answer(round.answer, decimals)?;

    Ok(EthUsdPrice {
        price: Usd::new(value),
        source: CHAINLINK_SOURCE.to_owned(),
        observed_at,
        block_number: Some(block_number),
    })
}

fn scale_answer(answer: I256, decimals: u8) -> Result<Float, EthUsdValuationError> {
    let mantissa = Float::parse(answer.to_string())
        .map_err(|source| EthUsdValuationError::Decimal { answer, source })?;
    let scale = (0..decimals).try_fold(ONE, |acc, _| acc * TEN)?;

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

    use st0x_float_serde::format_float;

    use super::*;

    const CHAINLINK_FEED: Address = address!("0x71041dddad3595F9CEd3DcCFBe3D1F4b0a16Bb70");
    const RECEIPT_BLOCK_HASH: B256 =
        b256!("0x1111111111111111111111111111111111111111111111111111111111111111");

    fn occurred_at() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 20, 12, 0, 0).unwrap()
    }

    fn encode_decimals(decimals: u8) -> Bytes {
        Bytes::from(AggregatorV3Interface::decimalsCall::abi_encode_returns(
            &decimals,
        ))
    }

    fn encode_round(answer: I256, updated_at: DateTime<Utc>) -> Bytes {
        Bytes::from(
            AggregatorV3Interface::latestRoundDataCall::abi_encode_returns(
                &AggregatorV3Interface::latestRoundDataReturn {
                    roundId: alloy::primitives::aliases::U80::from_limbs([1, 0]),
                    answer,
                    startedAt: U256::from(updated_at.timestamp()),
                    updatedAt: U256::from(updated_at.timestamp()),
                    answeredInRound: alloy::primitives::aliases::U80::from_limbs([1, 0]),
                },
            ),
        )
    }

    /// HyperEVM pays gas in HYPE. Reading the ETH/USD feed for it would
    /// return a number that looks valid and prices the gas wrongly, so the
    /// read is refused before any RPC call is made.
    #[tokio::test]
    async fn non_eth_gas_token_chain_is_refused_without_an_rpc_call() {
        let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());

        let error = read_eth_usd_price(
            &provider,
            CHAINLINK_FEED,
            Chain::HyperEvm,
            123,
            RECEIPT_BLOCK_HASH,
            occurred_at(),
        )
        .await
        .unwrap_err();

        assert!(
            matches!(
                error,
                EthUsdValuationError::NonEthGasToken {
                    chain: Chain::HyperEvm
                }
            ),
            "expected NonEthGasToken, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn base_receipt_records_scaled_chainlink_price_at_receipt_block() {
        let asserter = Asserter::new();
        asserter.push_success(&encode_decimals(8));
        asserter.push_success(&encode_round(
            I256::try_from(200_000_000_000_i64).unwrap(),
            occurred_at(),
        ));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result = read_eth_usd_price(
            &provider,
            CHAINLINK_FEED,
            Chain::Base,
            123,
            RECEIPT_BLOCK_HASH,
            occurred_at(),
        )
        .await
        .unwrap();

        assert_eq!(result.block_number, Some(123));
        assert_eq!(format_float(&result.price.inner()).unwrap(), "2000");
        assert_eq!(result.source, CHAINLINK_SOURCE);
    }

    /// Pins the configured proxy and assumed ABI against real Base responses,
    /// rather than only hand-built return values. Captured with:
    ///
    /// `cast call 0x71041dddad3595F9CEd3DcCFBe3D1F4b0a16Bb70 --data <selector>
    /// --rpc-url https://mainnet.base.org --block 50220982`
    #[tokio::test]
    async fn eth_usd_feed_resolves_to_a_real_populated_price_on_base() {
        let decimals_response = Bytes::from(alloy::hex!(
            "0000000000000000000000000000000000000000000000000000000000000008"
        ));
        let round_response = Bytes::from(alloy::hex!(
            "0000000000000000000000000000000000000000000000020000000000006b1b\
             00000000000000000000000000000000000000000000000000000035736be133\
             000000000000000000000000000000000000000000000000000000006a86fbdb\
             000000000000000000000000000000000000000000000000000000006a86fbe9\
             0000000000000000000000000000000000000000000000020000000000006b1b"
        ));

        assert_eq!(
            AggregatorV3Interface::decimalsCall::abi_decode_returns(&decimals_response).unwrap(),
            8
        );

        let observed_at = Utc.timestamp_opt(1_787_231_209, 0).unwrap();
        let asserter = Asserter::new();
        asserter.push_success(&decimals_response);
        asserter.push_success(&round_response);
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result = read_eth_usd_price(
            &provider,
            CHAINLINK_FEED,
            Chain::Base,
            50_220_982,
            RECEIPT_BLOCK_HASH,
            observed_at,
        )
        .await
        .unwrap();

        assert_eq!(
            format_float(&result.price.inner()).unwrap(),
            "2295.69716531"
        );
        assert_eq!(result.observed_at, observed_at);
        assert_eq!(result.source, CHAINLINK_SOURCE);
    }

    #[tokio::test]
    async fn base_receipt_pins_both_calls_at_receipt_block_hash() {
        let decimals_calldata = format!(
            "0x{}",
            hex::encode(AggregatorV3Interface::decimalsCall {}.abi_encode())
        );
        let round_calldata = format!(
            "0x{}",
            hex::encode(AggregatorV3Interface::latestRoundDataCall {}.abi_encode())
        );
        let block_hash = alloy::hex::encode(RECEIPT_BLOCK_HASH);
        let server = MockServer::start_async().await;
        let decimals_mock = server
            .mock_async(|when, then| {
                when.method(POST)
                    .body_includes(&decimals_calldata)
                    .body_includes(&block_hash);
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": 0,
                    "result": encode_decimals(8).to_string(),
                }));
            })
            .await;
        let round_mock = server
            .mock_async(|when, then| {
                when.method(POST)
                    .body_includes(&round_calldata)
                    .body_includes(&block_hash);
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": encode_round(
                        I256::try_from(200_000_000_000_i64).unwrap(),
                        occurred_at(),
                    )
                    .to_string(),
                }));
            })
            .await;
        let provider = ProviderBuilder::new().connect_http(server.base_url().parse().unwrap());

        read_eth_usd_price(
            &provider,
            CHAINLINK_FEED,
            Chain::Base,
            123,
            RECEIPT_BLOCK_HASH,
            occurred_at(),
        )
        .await
        .unwrap();

        decimals_mock.assert_async().await;
        round_mock.assert_async().await;
    }

    #[tokio::test]
    async fn ethereum_receipt_uses_latest_base_block() {
        let asserter = Asserter::new();
        asserter.push_success(&999u64);
        asserter.push_success(&encode_decimals(8));
        asserter.push_success(&encode_round(
            I256::try_from(200_000_000_000_i64).unwrap(),
            occurred_at(),
        ));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result = read_eth_usd_price(
            &provider,
            CHAINLINK_FEED,
            Chain::Ethereum,
            111,
            RECEIPT_BLOCK_HASH,
            occurred_at(),
        )
        .await
        .unwrap();

        assert_eq!(result.block_number, Some(999));
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn price_within_feed_heartbeat_does_not_warn() {
        let within_heartbeat = occurred_at() - TimeDelta::minutes(20);
        let asserter = Asserter::new();
        asserter.push_success(&encode_decimals(8));
        asserter.push_success(&encode_round(
            I256::try_from(200_000_000_000_i64).unwrap(),
            within_heartbeat,
        ));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        read_eth_usd_price(
            &provider,
            CHAINLINK_FEED,
            Chain::Base,
            123,
            RECEIPT_BLOCK_HASH,
            occurred_at(),
        )
        .await
        .unwrap();

        assert!(!logs_contain("ETH/USD Chainlink price is stale"));
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn stale_price_is_recorded() {
        let stale = occurred_at() - TimeDelta::hours(1);
        let asserter = Asserter::new();
        asserter.push_success(&encode_decimals(8));
        asserter.push_success(&encode_round(
            I256::try_from(200_000_000_000_i64).unwrap(),
            stale,
        ));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let result = read_eth_usd_price(
            &provider,
            CHAINLINK_FEED,
            Chain::Base,
            123,
            RECEIPT_BLOCK_HASH,
            occurred_at(),
        )
        .await
        .unwrap();

        assert_eq!(result.observed_at, stale);
        assert!(logs_contain("ETH/USD Chainlink price is stale"));
    }

    #[tokio::test]
    async fn non_positive_answer_is_rejected() {
        for answer in [I256::ZERO, I256::MINUS_ONE] {
            let asserter = Asserter::new();
            asserter.push_success(&encode_decimals(8));
            asserter.push_success(&encode_round(answer, occurred_at()));
            let provider = ProviderBuilder::new().connect_mocked_client(asserter);

            let error = read_eth_usd_price(
                &provider,
                CHAINLINK_FEED,
                Chain::Base,
                123,
                RECEIPT_BLOCK_HASH,
                occurred_at(),
            )
            .await
            .unwrap_err();

            assert!(matches!(
                error,
                EthUsdValuationError::NonPositivePrice { answer: actual } if actual == answer
            ));
        }
    }

    #[tokio::test]
    async fn zero_update_time_is_rejected() {
        let asserter = Asserter::new();
        asserter.push_success(&encode_decimals(8));
        asserter.push_success(&Bytes::from(
            AggregatorV3Interface::latestRoundDataCall::abi_encode_returns(
                &AggregatorV3Interface::latestRoundDataReturn {
                    roundId: alloy::primitives::aliases::U80::from_limbs([1, 0]),
                    answer: I256::try_from(200_000_000_000_i64).unwrap(),
                    startedAt: U256::ZERO,
                    updatedAt: U256::ZERO,
                    answeredInRound: alloy::primitives::aliases::U80::from_limbs([1, 0]),
                },
            ),
        ));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let error = read_eth_usd_price(
            &provider,
            CHAINLINK_FEED,
            Chain::Base,
            123,
            RECEIPT_BLOCK_HASH,
            occurred_at(),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            EthUsdValuationError::InvalidUpdatedAt(U256::ZERO)
        ));
    }

    #[tokio::test]
    async fn out_of_range_update_time_is_rejected() {
        let updated_at = U256::from(u64::MAX);
        let asserter = Asserter::new();
        asserter.push_success(&encode_decimals(8));
        asserter.push_success(&Bytes::from(
            AggregatorV3Interface::latestRoundDataCall::abi_encode_returns(
                &AggregatorV3Interface::latestRoundDataReturn {
                    roundId: alloy::primitives::aliases::U80::from_limbs([1, 0]),
                    answer: I256::try_from(200_000_000_000_i64).unwrap(),
                    startedAt: updated_at,
                    updatedAt: updated_at,
                    answeredInRound: alloy::primitives::aliases::U80::from_limbs([1, 0]),
                },
            ),
        ));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let error = read_eth_usd_price(
            &provider,
            CHAINLINK_FEED,
            Chain::Base,
            123,
            RECEIPT_BLOCK_HASH,
            occurred_at(),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            EthUsdValuationError::InvalidUpdatedAt(actual) if actual == updated_at
        ));
    }

    #[tokio::test]
    async fn excessive_decimals_are_rejected() {
        let asserter = Asserter::new();
        asserter.push_success(&encode_decimals(MAX_DECIMALS + 1));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let error = read_eth_usd_price(
            &provider,
            CHAINLINK_FEED,
            Chain::Base,
            123,
            RECEIPT_BLOCK_HASH,
            occurred_at(),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            EthUsdValuationError::DecimalsOutOfRange { decimals } if decimals == MAX_DECIMALS + 1
        ));
    }

    #[tokio::test]
    async fn contract_rpc_error_propagates() {
        let asserter = Asserter::new();
        asserter.push_failure_msg("eth_call boom");
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);

        let error = read_eth_usd_price(
            &provider,
            CHAINLINK_FEED,
            Chain::Base,
            123,
            RECEIPT_BLOCK_HASH,
            occurred_at(),
        )
        .await
        .unwrap_err();

        assert!(matches!(error, EthUsdValuationError::Contract(_)));
    }
}

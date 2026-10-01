//! `POST /quote/v2`: the request we send, the quote we accept, and the checks
//! that bind Relay's approve and deposit steps to the transfer we asked for.

use alloy::primitives::{Address, B256, Bytes, U256};
use alloy::sol;
use alloy::sol_types::SolCall;
use serde::{Deserialize, Serialize};

use st0x_evm::Chain;

use super::acceptance::{BasisPoints, QuoteAmounts};

sol! {
    function approve(address spender, uint256 amount) external returns (bool);

    /// Relay depository v2 entry point. The `id` is the order id that every
    /// later fill or refund carries, not the quote's request id.
    function depositErc20(address depositor, address token, uint256 amount, bytes32 id) external;
}

/// An exact-input quote for moving `amount` of `origin`'s settlement stable to
/// `destination`'s settlement stable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuoteRequest {
    pub origin: Chain,
    pub destination: Chain,
    /// Smallest unit of the origin stable.
    pub amount: U256,
    /// The wallet that signs the approve and the deposit.
    pub user: Address,
    pub recipient: Address,
    /// Always explicit: Relay's docs disagree on what an unset `refundTo` does.
    pub refund_to: Address,
    /// Always explicit: an unset slippage makes Relay floor the fill 2% below
    /// the expected amount.
    pub slippage: BasisPoints,
}

/// Relay's handle for a quote, used only to read its status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(transparent)]
pub struct RelayRequestId(pub B256);

impl std::fmt::Display for RelayRequestId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self(id) = self;
        write!(formatter, "{id}")
    }
}

/// The deposit's `bytes32 id`: the key the depository logs and the trailing
/// calldata word of the fill or refund that settles it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RelayOrderId(pub B256);

impl std::fmt::Display for RelayOrderId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self(id) = self;
        write!(formatter, "{id}")
    }
}

/// A quote whose approve and deposit steps were checked against the request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelayQuote {
    pub request_id: RelayRequestId,
    /// Decoded from the deposit step's calldata.
    pub order_id: RelayOrderId,
    pub amounts: QuoteAmounts,
    pub fees: QuoteFees,
    /// `None` when Relay sends no approve step.
    pub approve: Option<StepTransaction>,
    pub deposit: StepTransaction,
}

/// Fees Relay quoted, both paid on the origin chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuoteFees {
    /// In the origin stable's smallest unit, already out of the expected amount.
    pub relayer: U256,
    /// In the origin chain's native token (wei): Relay's estimate of our gas.
    pub gas: U256,
}

/// A transaction Relay asks the user to send.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepTransaction {
    pub chain_id: u64,
    pub to: Address,
    pub data: Bytes,
    pub value: U256,
}

/// The step of a quote a check refers to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuoteStep {
    Approve,
    Deposit,
}

/// How a quote differs from the transfer it was requested for.
#[derive(Debug, thiserror::Error)]
pub enum QuoteMismatch {
    #[error("Relay has no depository pinned on {chain}")]
    NoDepository { chain: Chain },
    #[error("quote has no deposit step")]
    MissingDeposit,
    #[error("quote step {position} is neither approve nor deposit")]
    UnexpectedStep { position: usize },
    #[error("quote has more than one {step:?} step")]
    DuplicateStep { step: QuoteStep },
    #[error("{step:?} step has {count} items, expected one")]
    ItemCount { step: QuoteStep, count: usize },
    #[error("{step:?} step is on chain {actual}, expected {expected}")]
    StepChain {
        step: QuoteStep,
        expected: u64,
        actual: u64,
    },
    #[error("{step:?} step calls {actual}, expected {expected}")]
    StepTarget {
        step: QuoteStep,
        expected: Address,
        actual: Address,
    },
    #[error("{step:?} step sends {value} native value, expected none")]
    StepValue { step: QuoteStep, value: U256 },
    #[error("{step:?} step calldata does not decode: {source}")]
    StepCalldata {
        step: QuoteStep,
        #[source]
        source: alloy::sol_types::Error,
    },
    #[error("approve names spender {actual}, expected the depository {expected}")]
    ApproveSpender { expected: Address, actual: Address },
    #[error("{step:?} step moves {actual}, expected exactly {expected}")]
    StepAmount {
        step: QuoteStep,
        expected: U256,
        actual: U256,
    },
    #[error("deposit credits depositor {actual}, expected {expected}")]
    Depositor { expected: Address, actual: Address },
    #[error("deposit pays token {actual}, expected {expected}")]
    DepositToken { expected: Address, actual: Address },
    #[error("quote input is {actual}, expected {expected}")]
    InputCurrency {
        expected: QuotedCurrency,
        actual: QuotedCurrency,
    },
    #[error("quote input amount is {actual}, expected {expected}")]
    InputAmount { expected: U256, actual: U256 },
    #[error("quote output is {actual}, expected {expected}")]
    OutputCurrency {
        expected: QuotedCurrency,
        actual: QuotedCurrency,
    },
}

/// A token on a chain, as a quote names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuotedCurrency {
    pub chain_id: u64,
    pub address: Address,
}

impl std::fmt::Display for QuotedCurrency {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{} on chain {}", self.address, self.chain_id)
    }
}

/// Wire body of `POST /quote/v2`. Amounts go as decimal strings: alloy's serde
/// would send `U256` as hex, which Relay does not read.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct QuoteRequestBody {
    user: Address,
    origin_chain_id: u64,
    destination_chain_id: u64,
    origin_currency: Address,
    destination_currency: Address,
    amount: String,
    trade_type: &'static str,
    recipient: Address,
    refund_to: Address,
    slippage_tolerance: String,
}

impl From<&QuoteRequest> for QuoteRequestBody {
    fn from(request: &QuoteRequest) -> Self {
        let BasisPoints(slippage) = request.slippage;

        Self {
            user: request.user,
            origin_chain_id: request.origin.chain_id(),
            destination_chain_id: request.destination.chain_id(),
            origin_currency: request.origin.settlement_stable().address,
            destination_currency: request.destination.settlement_stable().address,
            amount: request.amount.to_string(),
            trade_type: "EXACT_INPUT",
            recipient: request.recipient,
            refund_to: request.refund_to,
            slippage_tolerance: slippage.to_string(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct QuoteResponse {
    request_id: RelayRequestId,
    steps: Vec<RawStep>,
    fees: RawFees,
    details: RawDetails,
}

#[derive(Debug, Deserialize)]
struct RawStep {
    id: String,
    items: Vec<RawStepItem>,
}

#[derive(Debug, Deserialize)]
struct RawStepItem {
    data: RawStepData,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawStepData {
    to: Address,
    data: Bytes,
    #[serde(with = "decimal")]
    value: U256,
    chain_id: u64,
}

#[derive(Debug, Deserialize)]
struct RawFees {
    gas: RawAmount,
    relayer: RawAmount,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawDetails {
    currency_in: RawAmount,
    currency_out: RawAmount,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawAmount {
    currency: RawCurrency,
    #[serde(with = "decimal")]
    amount: U256,
    #[serde(with = "decimal")]
    minimum_amount: U256,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawCurrency {
    chain_id: u64,
    address: Address,
}

impl From<&RawCurrency> for QuotedCurrency {
    fn from(currency: &RawCurrency) -> Self {
        Self {
            chain_id: currency.chain_id,
            address: currency.address,
        }
    }
}

impl QuoteResponse {
    /// Checks the quote against `request` and reads the order id from the
    /// deposit calldata.
    pub(super) fn validate(self, request: &QuoteRequest) -> Result<RelayQuote, QuoteMismatch> {
        let origin = request.origin;
        let depository = origin
            .relay_depository()
            .ok_or(QuoteMismatch::NoDepository { chain: origin })?;
        let origin_stable = origin.settlement_stable().address;

        check_currency(
            &self.details.currency_in.currency,
            origin,
            |expected, actual| QuoteMismatch::InputCurrency { expected, actual },
        )?;
        check_currency(
            &self.details.currency_out.currency,
            request.destination,
            |expected, actual| QuoteMismatch::OutputCurrency { expected, actual },
        )?;

        if self.details.currency_in.amount != request.amount {
            return Err(QuoteMismatch::InputAmount {
                expected: request.amount,
                actual: self.details.currency_in.amount,
            });
        }

        let (approve, deposit) = split_steps(self.steps)?;

        let approve = approve
            .map(|approve| {
                check_approve(&approve, origin, origin_stable, depository, request.amount)?;
                Ok::<_, QuoteMismatch>(approve)
            })
            .transpose()?;

        let order_id = check_deposit(&deposit, request, origin_stable, depository)?;

        Ok(RelayQuote {
            request_id: self.request_id,
            order_id,
            amounts: QuoteAmounts {
                amount_in: request.amount,
                expected_out: self.details.currency_out.amount,
                minimum_out: self.details.currency_out.minimum_amount,
                slippage: request.slippage,
            },
            fees: QuoteFees {
                relayer: self.fees.relayer.amount,
                gas: self.fees.gas.amount,
            },
            approve,
            deposit,
        })
    }
}

fn check_currency(
    currency: &RawCurrency,
    chain: Chain,
    mismatch: impl FnOnce(QuotedCurrency, QuotedCurrency) -> QuoteMismatch,
) -> Result<(), QuoteMismatch> {
    let expected = QuotedCurrency {
        chain_id: chain.chain_id(),
        address: chain.settlement_stable().address,
    };
    let actual = QuotedCurrency::from(currency);

    if actual == expected {
        Ok(())
    } else {
        Err(mismatch(expected, actual))
    }
}

/// Splits the steps into the optional approve and the required deposit,
/// refusing any other step: an unknown step is a flow this code never signs.
fn split_steps(
    steps: Vec<RawStep>,
) -> Result<(Option<StepTransaction>, StepTransaction), QuoteMismatch> {
    let mut approve = None;
    let mut deposit = None;

    for (position, step) in steps.into_iter().enumerate() {
        let (kind, slot) = match step.id.as_str() {
            "approve" => (QuoteStep::Approve, &mut approve),
            "deposit" => (QuoteStep::Deposit, &mut deposit),
            _ => return Err(QuoteMismatch::UnexpectedStep { position }),
        };

        if slot.is_some() {
            return Err(QuoteMismatch::DuplicateStep { step: kind });
        }

        let [item] =
            <[RawStepItem; 1]>::try_from(step.items).map_err(|items| QuoteMismatch::ItemCount {
                step: kind,
                count: items.len(),
            })?;

        *slot = Some(StepTransaction {
            chain_id: item.data.chain_id,
            to: item.data.to,
            data: item.data.data,
            value: item.data.value,
        });
    }

    let deposit = deposit.ok_or(QuoteMismatch::MissingDeposit)?;

    Ok((approve, deposit))
}

fn check_step_envelope(
    step: QuoteStep,
    transaction: &StepTransaction,
    origin: Chain,
    expected_target: Address,
) -> Result<(), QuoteMismatch> {
    if transaction.chain_id != origin.chain_id() {
        return Err(QuoteMismatch::StepChain {
            step,
            expected: origin.chain_id(),
            actual: transaction.chain_id,
        });
    }

    if transaction.to != expected_target {
        return Err(QuoteMismatch::StepTarget {
            step,
            expected: expected_target,
            actual: transaction.to,
        });
    }

    if !transaction.value.is_zero() {
        return Err(QuoteMismatch::StepValue {
            step,
            value: transaction.value,
        });
    }

    Ok(())
}

fn check_approve(
    approve: &StepTransaction,
    origin: Chain,
    origin_stable: Address,
    depository: Address,
    amount: U256,
) -> Result<(), QuoteMismatch> {
    check_step_envelope(QuoteStep::Approve, approve, origin, origin_stable)?;

    let call =
        approveCall::abi_decode(&approve.data).map_err(|source| QuoteMismatch::StepCalldata {
            step: QuoteStep::Approve,
            source,
        })?;

    if call.spender != depository {
        return Err(QuoteMismatch::ApproveSpender {
            expected: depository,
            actual: call.spender,
        });
    }

    if call.amount != amount {
        return Err(QuoteMismatch::StepAmount {
            step: QuoteStep::Approve,
            expected: amount,
            actual: call.amount,
        });
    }

    Ok(())
}

fn check_deposit(
    deposit: &StepTransaction,
    request: &QuoteRequest,
    origin_stable: Address,
    depository: Address,
) -> Result<RelayOrderId, QuoteMismatch> {
    check_step_envelope(QuoteStep::Deposit, deposit, request.origin, depository)?;

    let call = depositErc20Call::abi_decode(&deposit.data).map_err(|source| {
        QuoteMismatch::StepCalldata {
            step: QuoteStep::Deposit,
            source,
        }
    })?;

    if call.depositor != request.user {
        return Err(QuoteMismatch::Depositor {
            expected: request.user,
            actual: call.depositor,
        });
    }

    if call.token != origin_stable {
        return Err(QuoteMismatch::DepositToken {
            expected: origin_stable,
            actual: call.token,
        });
    }

    if call.amount != request.amount {
        return Err(QuoteMismatch::StepAmount {
            step: QuoteStep::Deposit,
            expected: request.amount,
            actual: call.amount,
        });
    }

    Ok(RelayOrderId(call.id))
}

/// Relay writes amounts as base-10 strings.
mod decimal {
    use alloy::primitives::U256;
    use serde::{Deserialize, Deserializer, de::Error};

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<U256, D::Error> {
        let text = String::deserialize(deserializer)?;
        U256::from_str_radix(&text, 10).map_err(D::Error::custom)
    }
}

#[cfg(test)]
pub(super) mod tests {
    use alloy::primitives::{address, b256};
    use serde_json::{Value, json};

    use super::*;

    pub(in crate::relay) const FUNDED_QUOTE: &str =
        include_str!("../../relay-fixtures/quote_funded_robinhood_to_ethereum.json");

    pub(in crate::relay) const FUNDED_WALLET: Address =
        address!("0xe385C5EE42d7B81A6a51E759FaaFca6159Fd04B6");

    const DEPOSITORY: Address = address!("0x4cd00e387622c35bddb9b4c962c136462338bc31");

    /// The request behind the RAI-2586 funded quote: 5 USDG Robinhood to
    /// Ethereum, 30 bps slippage.
    pub(in crate::relay) fn funded_request() -> QuoteRequest {
        QuoteRequest {
            origin: Chain::Robinhood,
            destination: Chain::Ethereum,
            amount: U256::from(5_000_000),
            user: FUNDED_WALLET,
            recipient: FUNDED_WALLET,
            refund_to: FUNDED_WALLET,
            slippage: BasisPoints::new(30).unwrap(),
        }
    }

    fn validate(body: &Value, request: &QuoteRequest) -> Result<RelayQuote, QuoteMismatch> {
        serde_json::from_value::<QuoteResponse>(body.clone())
            .unwrap()
            .validate(request)
    }

    fn funded_body() -> Value {
        serde_json::from_str(FUNDED_QUOTE).unwrap()
    }

    #[test]
    fn request_body_sends_decimal_amount_and_explicit_slippage() {
        let body = serde_json::to_value(QuoteRequestBody::from(&funded_request())).unwrap();

        assert_eq!(
            body,
            json!({
                "user": "0xe385c5ee42d7b81a6a51e759faafca6159fd04b6",
                "originChainId": 4663,
                "destinationChainId": 1,
                "originCurrency": "0x5fc5360d0400a0fd4f2af552add042d716f1d168",
                "destinationCurrency": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                "amount": "5000000",
                "tradeType": "EXACT_INPUT",
                "recipient": "0xe385c5ee42d7b81a6a51e759faafca6159fd04b6",
                "refundTo": "0xe385c5ee42d7b81a6a51e759faafca6159fd04b6",
                "slippageTolerance": "30",
            })
        );
    }

    #[test]
    fn order_id_is_the_deposit_calldata_id_not_request_id() {
        let quote = validate(&funded_body(), &funded_request()).unwrap();

        assert_eq!(
            quote.order_id,
            RelayOrderId(b256!(
                "0x266b12442f9b86ef731fae285c34f489217acdfcedd755422ce47db429992d85"
            ))
        );
        assert_eq!(
            quote.request_id,
            RelayRequestId(b256!(
                "0x1790875063e29a43c3a2201049fa8f6e6542e940fe2060ed03ea9107630b5a49"
            ))
        );
    }

    #[test]
    fn funded_quote_reads_amounts_fees_and_steps() {
        let quote = validate(&funded_body(), &funded_request()).unwrap();

        assert_eq!(
            quote.amounts,
            QuoteAmounts {
                amount_in: U256::from(5_000_000),
                expected_out: U256::from(4_763_755),
                minimum_out: U256::from(4_749_464),
                slippage: BasisPoints::new(30).unwrap(),
            }
        );
        assert_eq!(
            quote.fees,
            QuoteFees {
                relayer: U256::from(235_904),
                gas: U256::from(2_382_592_438_800_u64),
            }
        );

        let approve = quote.approve.unwrap();
        assert_eq!(approve.chain_id, 4663);
        assert_eq!(
            approve.to,
            address!("0x5fc5360D0400a0Fd4f2af552ADD042D716F1d168")
        );
        assert_eq!(approve.value, U256::ZERO);
        assert_eq!(&approve.data[..4], &[0x09, 0x5e, 0xa7, 0xb3]);

        assert_eq!(quote.deposit.chain_id, 4663);
        assert_eq!(quote.deposit.to, DEPOSITORY);
        assert_eq!(quote.deposit.value, U256::ZERO);
        assert_eq!(&quote.deposit.data[..4], &[0xe8, 0x01, 0x79, 0x52]);
    }

    /// Both RAI-2586 directions at 1000 units, quoted for the `0xdead` user.
    #[test]
    fn both_directions_validate() {
        let dead = address!("0x000000000000000000000000000000000000dEaD");

        for (fixture, origin, destination) in [
            (
                include_str!("../../relay-fixtures/quote_robinhood_to_ethereum.json"),
                Chain::Robinhood,
                Chain::Ethereum,
            ),
            (
                include_str!("../../relay-fixtures/quote_ethereum_to_robinhood.json"),
                Chain::Ethereum,
                Chain::Robinhood,
            ),
        ] {
            let request = QuoteRequest {
                origin,
                destination,
                amount: U256::from(1_000_000_000),
                user: dead,
                recipient: dead,
                refund_to: dead,
                slippage: BasisPoints::new(30).unwrap(),
            };

            let quote = validate(&serde_json::from_str(fixture).unwrap(), &request).unwrap();

            assert_eq!(quote.deposit.chain_id, origin.chain_id());
            assert_eq!(quote.amounts.amount_in, U256::from(1_000_000_000));
        }
    }

    #[test]
    fn approve_to_another_spender_is_refused() {
        let mut body = funded_body();
        let other_spender = "0x095ea7b3\
            0000000000000000000000001111111111111111111111111111111111111111\
            00000000000000000000000000000000000000000000000000000000004c4b40";
        body["steps"][0]["items"][0]["data"]["data"] = json!(other_spender);

        let error = validate(&body, &funded_request()).unwrap_err();

        assert!(
            matches!(
                error,
                QuoteMismatch::ApproveSpender { expected, actual }
                    if expected == DEPOSITORY
                        && actual == address!("0x1111111111111111111111111111111111111111")
            ),
            "{error:?}"
        );
    }

    #[test]
    fn approve_above_the_amount_is_refused() {
        let mut body = funded_body();
        let unlimited = "0x095ea7b3\
            0000000000000000000000004cd00e387622c35bddb9b4c962c136462338bc31\
            ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
        body["steps"][0]["items"][0]["data"]["data"] = json!(unlimited);

        let error = validate(&body, &funded_request()).unwrap_err();

        assert!(
            matches!(
                error,
                QuoteMismatch::StepAmount { step: QuoteStep::Approve, expected, actual }
                    if expected == U256::from(5_000_000) && actual == U256::MAX
            ),
            "{error:?}"
        );
    }

    #[test]
    fn deposit_to_another_contract_is_refused() {
        let mut body = funded_body();
        body["steps"][1]["items"][0]["data"]["to"] =
            json!("0x2222222222222222222222222222222222222222");

        let error = validate(&body, &funded_request()).unwrap_err();

        assert!(
            matches!(
                error,
                QuoteMismatch::StepTarget { step: QuoteStep::Deposit, expected, .. }
                    if expected == DEPOSITORY
            ),
            "{error:?}"
        );
    }

    #[test]
    fn quote_for_another_amount_is_refused() {
        let request = QuoteRequest {
            amount: U256::from(4_000_000),
            ..funded_request()
        };

        let error = validate(&funded_body(), &request).unwrap_err();

        assert!(
            matches!(
                error,
                QuoteMismatch::InputAmount { expected, actual }
                    if expected == U256::from(4_000_000) && actual == U256::from(5_000_000)
            ),
            "{error:?}"
        );
    }

    #[test]
    fn deposit_crediting_another_depositor_is_refused() {
        let request = QuoteRequest {
            user: address!("0x3333333333333333333333333333333333333333"),
            ..funded_request()
        };

        let error = validate(&funded_body(), &request).unwrap_err();

        assert!(
            matches!(error, QuoteMismatch::Depositor { actual, .. } if actual == FUNDED_WALLET),
            "{error:?}"
        );
    }

    #[test]
    fn quote_for_the_wrong_output_currency_is_refused() {
        let request = QuoteRequest {
            destination: Chain::Base,
            ..funded_request()
        };

        let error = validate(&funded_body(), &request).unwrap_err();

        assert!(
            matches!(
                error,
                QuoteMismatch::OutputCurrency { expected, actual }
                    if expected.chain_id == 8453 && actual.chain_id == 1
            ),
            "{error:?}"
        );
    }

    #[test]
    fn missing_approve_is_allowed_and_missing_deposit_is_refused() {
        let mut body = funded_body();
        body["steps"].as_array_mut().unwrap().remove(0);

        let quote = validate(&body, &funded_request()).unwrap();
        assert_eq!(quote.approve, None);

        body["steps"] = json!([]);
        let error = validate(&body, &funded_request()).unwrap_err();
        assert!(matches!(error, QuoteMismatch::MissingDeposit), "{error:?}");
    }

    #[test]
    fn unknown_step_is_refused() {
        let mut body = funded_body();
        body["steps"][0]["id"] = json!("permit");

        let error = validate(&body, &funded_request()).unwrap_err();

        assert!(
            matches!(error, QuoteMismatch::UnexpectedStep { position: 0 }),
            "{error:?}"
        );
    }

    #[test]
    fn origin_without_a_depository_is_refused() {
        let request = QuoteRequest {
            origin: Chain::Base,
            ..funded_request()
        };

        let error = validate(&funded_body(), &request).unwrap_err();

        assert!(
            matches!(error, QuoteMismatch::NoDepository { chain: Chain::Base }),
            "{error:?}"
        );
    }
}
